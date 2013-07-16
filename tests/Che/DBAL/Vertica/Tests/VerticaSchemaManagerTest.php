<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica\Tests;

use Che\DBAL\Vertica\VerticaDriver;
use Doctrine\Common\EventManager;
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Events;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Types\Type;
use Doctrine\Tests\DBAL\Functional\Schema\SchemaManagerFunctionalTestCase;

/**
 * Functional schema manager test for VerticaSchemaManager
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class VerticaSchemaManagerTest extends SchemaManagerFunctionalTestCase
{
    protected function setUp()
    {
        $this->_conn = new Connection(
            [
                'host' => $GLOBALS['db_host'],
                'port' => $GLOBALS['db_port'],
                'user' => $GLOBALS['db_username'],
                'password' => $GLOBALS['db_password'],
                'dbname' => $GLOBALS['db_name']
            ],
            new VerticaDriver(),
            new Configuration()
        );

        $this->_sm = $this->_conn->getSchemaManager();
        $this->_conn->exec('DROP SCHEMA IF EXISTS doctrine CASCADE');
        $this->_conn->exec('CREATE SCHEMA doctrine');
        $this->_conn->exec('SET search_path TO "$user", doctrine, public, v_catalog, v_monitor, v_internal');
    }

    protected function tearDown()
    {
        $this->_conn->exec('DROP SCHEMA doctrine CASCADE');
    }

    public function testListTableIndexesDispatchEvent()
    {
        $table = $this->getTestTable('list_table_indexes_test');
        $table->addUniqueIndex(['id', 'test'], 'test_index_name');

        $this->_sm->dropAndCreateTable($table);

        $listenerMock = $this->getMock('ListTableIndexesDispatchEventListener', ['onSchemaIndexDefinition']);
        $listenerMock
            ->expects($this->exactly(2))
            ->method('onSchemaIndexDefinition');

        $oldEventManager = $this->_sm->getDatabasePlatform()->getEventManager();

        $eventManager = new EventManager();
        $eventManager->addEventListener([Events::onSchemaIndexDefinition], $listenerMock);

        $this->_sm->getDatabasePlatform()->setEventManager($eventManager);

        $this->_sm->listTableIndexes('list_table_indexes_test');

        $this->_sm->getDatabasePlatform()->setEventManager($oldEventManager);
    }

    public function testDiffListTableColumns()
    {
        if ($this->_sm->getDatabasePlatform()->getName() == 'oracle') {
            $this->markTestSkipped('Does not work with Oracle, since it cannot detect DateTime, Date and Time differenecs (at the moment).');
        }

        $offlineTable = $this->createListTableColumns();
        $this->_sm->dropAndCreateTable($offlineTable);
        $onlineTable = $this->_sm->listTableDetails('list_table_columns');

        $comparator = new Comparator();
        $diff = $comparator->diffTable($offlineTable, $onlineTable);

        $this->assertFalse($diff, "No differences should be detected with the offline vs online schema.");
    }

    public function testListTableIndexes()
    {
        $table = $this->getTestCompositeTable('list_table_indexes_test');
        $table->addUniqueIndex(['id', 'test'], 'test_index_name');

        $this->_sm->dropAndCreateTable($table);

        $tableIndexes = $this->_sm->listTableIndexes('list_table_indexes_test');

        $this->assertEquals(2, count($tableIndexes));

        $this->assertArrayHasKey('primary', $tableIndexes, 'listTableIndexes() has to return a "primary" array key.');
        $this->assertEquals(['id', 'other_id'], array_map('strtolower', $tableIndexes['primary']->getColumns()));
        $this->assertTrue($tableIndexes['primary']->isUnique());
        $this->assertTrue($tableIndexes['primary']->isPrimary());

        $this->assertEquals('test_index_name', $tableIndexes['test_index_name']->getName());
        $this->assertEquals(['id', 'test'], array_map('strtolower', $tableIndexes['test_index_name']->getColumns()));
        $this->assertTrue($tableIndexes['test_index_name']->isUnique());
        $this->assertFalse($tableIndexes['test_index_name']->isPrimary());
    }

    public function testDropAndCreateIndex()
    {
        $this->indexesNotSupported();
    }

    public function testAutoincrementDetection()
    {
        if (!$this->_sm->getDatabasePlatform()->supportsIdentityColumns()) {
            $this->markTestSkipped('This test is only supported on platforms that have autoincrement');
        }

        $table = new \Doctrine\DBAL\Schema\Table('test_autoincrement');
        $table->setSchemaConfig($this->_sm->createSchemaConfig());
        $table->addColumn('id', 'integer', ['autoincrement' => true]);
        $table->addColumn('test', 'integer');
        $table->setPrimaryKey(['id']);

        $this->_sm->createTable($table);

        $inferredTable = $this->_sm->listTableDetails('test_autoincrement');
        $this->assertTrue($inferredTable->hasColumn('id'));
        $this->assertTrue($inferredTable->getColumn('id')->getAutoincrement());
    }

    public function testAlterTableScenario()
    {
        $this->createTestTable('alter_table');
        $this->createTestTable('alter_table_foreign');

        $table = $this->_sm->listTableDetails('alter_table');
        $this->assertTrue($table->hasColumn('id'));
        $this->assertTrue($table->hasColumn('test'));
        $this->assertTrue($table->hasColumn('foreign_key_test'));
        $this->assertEquals(0, count($table->getForeignKeys()));
        $this->assertEquals(1, count($table->getIndexes()));

        $tableDiff = new TableDiff("alter_table");
        $tableDiff->addedColumns['foo'] = new Column('foo', Type::getType('integer'));
        $tableDiff->removedColumns['test'] = $table->getColumn('test');

        $this->_sm->alterTable($tableDiff);

        $table = $this->_sm->listTableDetails('alter_table');
        $this->assertFalse($table->hasColumn('test'));
        $this->assertTrue($table->hasColumn('foo'));

        $tableDiff = new TableDiff("alter_table");
        $fk = new ForeignKeyConstraint(['foreign_key_test'], 'alter_table_foreign', ['id']);
        $tableDiff->addedForeignKeys[] = $fk;

        $this->_sm->alterTable($tableDiff);
        $table = $this->_sm->listTableDetails('alter_table');

        $this->assertEquals(1, count($table->getForeignKeys()));
        $fks = $table->getForeignKeys();
        $foreignKey = current($fks);
        $this->assertEquals('alter_table_foreign', strtolower($foreignKey->getForeignTableName()));
        $this->assertEquals(['foreign_key_test'], array_map('strtolower', $foreignKey->getColumns()));
        $this->assertEquals(['id'], array_map('strtolower', $foreignKey->getForeignColumns()));
    }

    public function testUpdateSchemaWithForeignKeyRenaming()
    {
        $this->indexesNotSupported();
    }

    /**
     * @group DBAL-177
     */
    public function testGetSearchPath()
    {
        $paths = $this->_sm->getSchemaSearchPaths();
        $this->assertEquals(['dbadmin', 'doctrine', 'public', 'v_catalog', 'v_monitor', 'v_internal'], $paths);
    }

    private function indexesNotSupported()
    {
        $this->markTestSkipped('Indexes are not supported');
    }
}
