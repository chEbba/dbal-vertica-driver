<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica;

use Doctrine\DBAL\Event\SchemaIndexDefinitionEventArgs;
use Doctrine\DBAL\Events;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\PostgreSqlSchemaManager;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;

/**
 * SchemaManager for Vertica
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class VerticaSchemaManager extends PostgreSqlSchemaManager
{
    /**
     * {@inheritDoc}
     */
    public function getSchemaNames()
    {
        $rows = $this->_conn->fetchAll("SELECT schema_name FROM v_catalog.schemata WHERE not is_system_schema");
        return array_map(function($v) { return $v['schema_name']; }, $rows);
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaSearchPaths()
    {
        $params = $this->_conn->getParams();
        $schema = explode(",", $this->_conn->fetchColumn('SHOW search_path', [], 1));

        if (isset($params['user'])) {
            $schema = str_replace('"$user"', $params['user'], $schema);
        }

        return array_map('trim', $schema);
    }

    /**
     * Convert platform list of foreign keys to portable format
     *
     * @param array $tableForeignKeys
     *
     * @return ForeignKeyConstraint[]
     *
     * @see AbstractSchemaManager::listTableForeignKeys()
     * @see VerticaPlatform::getListTableForeignKeysSQL()
     */
    protected function _getPortableTableForeignKeysList($tableForeignKeys)
    {
        // Multi-column FK are presented as several rows with same constraint_id
        $keys = [];
        foreach ($tableForeignKeys as $keyRow) {
            if (!isset($keys[$keyRow['constraint_id']])) {
                $keys[$keyRow['constraint_id']] = [
                    'localColumns'   => [$keyRow['column_name']],
                    'foreignTable'   => $keyRow['reference_table_name'],
                    'foreignColumns' => [$keyRow['reference_column_name']],
                    'name'           => $keyRow['constraint_name']
                ];
            } else {
                $keys[$keyRow['constraint_id']]['localColumns'][] = $keyRow['column_name'];
                $keys[$keyRow['constraint_id']]['foreignColumns'][] = $keyRow['reference_column_name'];
            }
        }

        return parent::_getPortableTableForeignKeysList($keys);
    }


    /**
     * @param array $tableForeignKey
     *
     * @return ForeignKeyConstraint
     *
     * @see _getPortableTableForeignKeysList
     */
    protected function _getPortableTableForeignKeyDefinition($tableForeignKey)
    {
        return new ForeignKeyConstraint(
            $tableForeignKey['localColumns'], $tableForeignKey['foreignTable'],
            $tableForeignKey['foreignColumns'], $tableForeignKey['name']
        );
    }

    /**
     * Convert platform results for sequence definition to a portable format
     *
     * @param array $sequence
     *
     * @return Sequence
     *
     * @see AbstractSchemaManager::listSequences()
     * @see VerticaPlatform::getListSequencesSQL()
     */
    protected function _getPortableSequenceDefinition($sequence)
    {
        return new Sequence($sequence['sequence_name'], $sequence['increment_by'], $sequence['minimum']);
    }

    /**
     * Converts platform sql result for column definition to a portable format
     *
     * @param array $tableColumn
     *
     * @return Column
     *
     * @see AbstractSchemaManager::listTableColumns()
     * @see VerticaPlatform::getListTableColumnsSQL()
     */
    protected function _getPortableTableColumnDefinition($tableColumn)
    {
        // Remove size declaration from type, ex. numeric(10,2) -> numeric
        $dbType = rtrim($tableColumn['data_type'], '()0123456789,');
        if ($dbType === 'varchar' && $tableColumn['character_maximum_length'] == 65000) {
            $dbType = 'text';
        }

        // Unescape default value
        if (preg_match("/^'(.*)'(::.*)?$/", $tableColumn['column_default'], $matches)) {
            $tableColumn['column_default'] = $matches[1];
        }

        if (stripos($tableColumn['column_default'], 'NULL') === 0) {
            $tableColumn['column_default'] = null;
        }

        $options = [
            'length'        => $tableColumn['character_maximum_length'],
            'notnull'       => !$tableColumn['is_nullable'],
            'default'       => $tableColumn['column_default'],
            'primary'       => $tableColumn['constraint_type'] == 'p',
            'precision'     => $tableColumn['numeric_precision'],
            'scale'         => $tableColumn['numeric_scale'],
            'fixed'         => $dbType == 'char' ? true : ($dbType == 'varchar' ? false : null),
            'unsigned'      => false,
            'autoincrement' => (bool) $tableColumn['is_identity'],
            'comment'       => '',
        ];

        $type = $this->_platform->getDoctrineTypeMapping($dbType);

        return new Column($tableColumn['column_name'], Type::getType($type), $options);
    }

    /**
     * Convert platform sql results for index definitions to a portable format
     *
     * @param array             $tableIndexRows
     * @param string|Table|null $tableName
     *
     * @return Index[]
     *
     * @see AbstractSchemaManager::listTableIndexes()
     * @see VerticaPlatform::getListTableIndexesSQL()
     */
    protected function _getPortableTableIndexesList($tableIndexRows, $tableName=null)
    {
        $result = [];
        foreach($tableIndexRows as $tableIndex) {
            $indexName = $keyName = $tableIndex['constraint_name'];
            if ($tableIndex['constraint_type'] == 'p') {
                $keyName = 'primary';
            }
            $keyName = strtolower($keyName);

            if (!isset($result[$keyName])) {
                $result[$keyName] = [
                    'name' => $indexName,
                    'columns' => [$tableIndex['column_name']],
                    'unique' => true, // we have only primary and unique constraints,
                    'primary' => $tableIndex['constraint_type'] == 'p'
                ];
            } else {
                $result[$keyName]['columns'][] = $tableIndex['column_name'];
            }
        }

        $eventManager = $this->_platform->getEventManager();

        $indexes = [];
        foreach($result as $indexKey => $data) {
            $index = null;
            $defaultPrevented = false;

            if (null !== $eventManager && $eventManager->hasListeners(Events::onSchemaIndexDefinition)) {
                $eventArgs = new SchemaIndexDefinitionEventArgs($data, $tableName, $this->_conn);
                $eventManager->dispatchEvent(Events::onSchemaIndexDefinition, $eventArgs);

                $defaultPrevented = $eventArgs->isDefaultPrevented();
                $index = $eventArgs->getIndex();
            }

            if ( ! $defaultPrevented) {
                $index = new Index($data['name'], $data['columns'], $data['unique'], $data['primary']);
            }

            if ($index) {
                $indexes[$indexKey] = $index;
            }
        }

        return $indexes;
    }
}
