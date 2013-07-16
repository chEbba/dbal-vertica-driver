<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica\Tests;

use Che\DBAL\Vertica\VerticaPlatform;
use Doctrine\Tests\DBAL\Platforms\AbstractPlatformTestCase;

/**
 * Test for VerticaPlatform
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class VerticaPlatformTest extends AbstractPlatformTestCase
{
    public function createPlatform()
    {
        return new VerticaPlatform();
    }

    public function getGenerateTableSql()
    {
        return 'CREATE TABLE test (id AUTO_INCREMENT NOT NULL, test VARCHAR(255) DEFAULT NULL, PRIMARY KEY(id))';
    }

    public function testGenerateTableWithMultiColumnUniqueIndex()
    {
        $this->markTestSkipped('Indexes is not supported by Vertica');
    }

    public function getGenerateTableWithMultiColumnUniqueIndexSql()
    {
        return [];
    }

    public function getGenerateAlterTableSql()
    {
        return [
            'ALTER TABLE mytable DROP foo',
            'ALTER TABLE mytable ALTER bar TYPE VARCHAR(255)',
            "ALTER TABLE mytable ALTER bar SET  DEFAULT 'def'",
            'ALTER TABLE mytable ALTER bar SET NOT NULL',
            'ALTER TABLE mytable ALTER bloo TYPE BOOLEAN',
            "ALTER TABLE mytable ALTER bloo SET  DEFAULT 'false'",
            'ALTER TABLE mytable ALTER bloo SET NOT NULL',
            'ALTER TABLE mytable RENAME TO userlist',
            'ALTER TABLE mytable ADD quota INT DEFAULT NULL'
        ];
    }

    public function testGeneratesIndexCreationSql()
    {
        $this->markTestSkipped('Vertica does not support indexes');
    }

    public function getGenerateIndexSql()
    {
        return '';
    }

    public function getGenerateForeignKeySql()
    {
        return 'ALTER TABLE test ADD FOREIGN KEY (fk_name_id) REFERENCES other_table (id)';
    }

    public function getGenerateUniqueIndexSql()
    {
        return 'ALTER TABLE test ADD CONSTRAINT index_name UNIQUE (test, test2)';
    }

    protected function getQuotedColumnInPrimaryKeySQL()
    {
        return ['CREATE TABLE "quoted" ("key" VARCHAR(255) NOT NULL, PRIMARY KEY("key"))'];
    }

    protected function getQuotedColumnInIndexSQL()
    {
        return [];
    }
}
