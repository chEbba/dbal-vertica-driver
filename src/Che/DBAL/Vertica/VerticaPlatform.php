<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica;

use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\Types\Type;

/**
 * DBAL Platform for Vertica
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class VerticaPlatform extends PostgreSqlPlatform
{
    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'vertica';
    }

    public function supportsCreateDropDatabase()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsCommentOnStatement()
    {
        // Vertica has COMMENT ON COLUMN|TABLE but column comments are supported only for projection columns
        // TODO: store db type comments in table comment?
        return false;
    }

    public function getCreateDatabaseSQL($name)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    public function getDropDatabaseSQL($database)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getListDatabasesSQL()
    {
        return "SELECT name as datname FROM v_catalog.databases";
    }

    public function getListSequencesSQL($database)
    {
        return "SELECT sequence_name, increment_by, minimum from v_catalog.sequences";
    }

    public function getListTablesSQL()
    {
        return "SELECT table_name, table_schema as schema_name FROM v_catalog.tables";
    }

    /**
     * {@inheritDoc}
     */
    public function getListViewsSQL($database)
    {
        return "SELECT table_name as viewname, view_definition as definition FROM v_catalog.views WHERE NOT is_system_view";
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableForeignKeysSQL($table, $database = null)
    {
        return sprintf(
            "SELECT constraint_id, constraint_name, column_name, reference_table_name, reference_column_name
                FROM v_catalog.foreign_keys
                WHERE table_name = '%s'",
            $table
        );
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableConstraintsSQL($table)
    {
        return "SELECT constraint_id, column_name, constraint_name, constraint_type FROM v_catalog.constraint_columns
                WHERE constraint_type IN ('p', 'u') AND table_name = '$table'";
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableIndexesSQL($table, $currentDatabase = null)
    {
        // There is no indexes in Vertica but doctrine treated constraints as indexes
        return $this->getListTableConstraintsSQL($table);
    }

    public function getCreateIndexSQL(Index $index, $table)
    {
        if ($index->isPrimary()) {
            return $this->getCreatePrimaryKeySQL($index, $table);
        }

        return $this->getCreateConstraintSQL($index, $table);
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableColumnsSQL($table, $database = null)
    {
        return sprintf(
            "SELECT col.column_name, col.data_type, col.character_maximum_length, col.numeric_precision, col.numeric_scale,
                    col.is_nullable, col.column_default, col.is_identity, con.constraint_type
                FROM v_catalog.columns col
                LEFT JOIN v_catalog.constraint_columns con ON
                    con.table_id = col.table_id AND con.column_name = col.column_name AND constraint_type = 'p'
                WHERE col.table_name = '%s'",
            $table
        );
    }

    /**
     * {@inheritDoc}
     */
    protected function _getCreateTableSQL($tableName, array $columns, array $options = [])
    {
        $queryFields = $this->getColumnDeclarationListSQL($columns);

        if (isset($options['primary']) && ! empty($options['primary'])) {
            $keyColumns = array_unique(array_values($options['primary']));
            $queryFields .= ', PRIMARY KEY(' . implode(', ', $keyColumns) . ')';
        }

        $query = 'CREATE TABLE ' . $tableName . ' (' . $queryFields . ')';

        $sql[] = $query;

        if (isset($options['indexes']) && ! empty($options['indexes'])) {
            /** @var Index $index */
            foreach ($options['indexes'] as $index) {
                if ($index->isUnique() && !$index->isPrimary())
                    $sql[] = $this->getCreateConstraintSQL($index, $tableName);
            }
        }

        if (isset($options['foreignKeys'])) {
            foreach ((array) $options['foreignKeys'] as $definition) {
                $sql[] = $this->getCreateForeignKeySQL($definition, $tableName);
            }
        }

        return $sql;
    }

    /**
     * {@inheritDoc}
     */
    protected function onSchemaAlterTableAddColumn(Column $column, TableDiff $diff, &$columnSql)
    {
        $defaultPrevented = parent::onSchemaAlterTableAddColumn($column, $diff, $columnSql);
        if ($defaultPrevented) {
            return true;
        }

        /** @var Column $column */
        foreach ($diff->addedColumns as $column) {
            $columnData = $column->toArray();
            $notNullWithoutDefault = !empty($columnData['notnull']) && !isset($columnData['default']);
            if ($notNullWithoutDefault) {
                $columnData['notnull'] = false;
            }

            $query = 'ADD ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnData);
            $columnSql[] = 'ALTER TABLE ' . $diff->name . ' ' . $query;
            if ($comment = $this->getColumnComment($column)) {
                $columnSql[] = $this->getCommentOnColumnSQL($diff->name, $column->getName(), $comment);
            }
            if ($notNullWithoutDefault) {
                $columnSql[] = 'ALTER TABLE ' . $diff->name . ' ALTER ' . $column->getQuotedName($this) . ' SET NOT NULL';
            }
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function getTruncateTableSQL($tableName, $cascade = false)
    {
        // Vertica does not support cascade
        return 'TRUNCATE TABLE '.$tableName;
    }

    /**
     * {@inheritDoc}
     */
    public function getReadLockSQL()
    {
        // Vertica supports only exclusive lock (FOR UPDATE)
        return $this->getForUpdateSQL();
    }

    /**
     * {@inheritDoc}
     */
    public function getAdvancedForeignKeyOptionsSQL(ForeignKeyConstraint $foreignKey)
    {
        // Vertica does not support any advanced options
        return '';
    }

    /**
     * {@inheritDoc}
     */
    public function getDropSequenceSQL($sequence)
    {
        if ($sequence instanceof Sequence) {
            $sequence = $sequence->getQuotedName($this);
        }
        return 'DROP SEQUENCE ' . $sequence;
    }

    /**
     * {@inheritDoc}
     */
    public function getIntegerTypeDeclarationSQL(array $field)
    {
        if (!empty($field['autoincrement'])) {
            return 'AUTO_INCREMENT';
        }

        return 'INT';
    }

    /**
     * {@inheritDoc}
     */
    public function getBigIntTypeDeclarationSQL(array $field)
    {
        // Vertica's int is a big int
        return $this->getIntegerTypeDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getSmallIntTypeDeclarationSQL(array $field)
    {
        // Vertica has only int
        return $this->getIntegerTypeDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getGuidTypeDeclarationSQL(array $field)
    {
        return $this->getVarcharTypeDeclarationSQL($field);
    }

    /**
     * {@inheritDoc}
     */
    public function getClobTypeDeclarationSQL(array $field)
    {
        // Vertica has only varchar with 65000 bytes, use it as text
        return 'VARCHAR(65000)';
    }

    /**
     * {@inheritDoc}
     */
    public function getBlobTypeDeclarationSQL(array $field)
    {
        // BYTEA is a synonym for VARBINARY, but use of VARBINARY is more clear
        return 'VARBINARY';
    }

    /**
     * {@inheritDoc}
     */
    public function getVarcharMaxLength()
    {
        // Vertica's VARCHAR has 65k bytes (not chars), 1 byte for clob and divide by 4 for utf-8 support
        return 65000/4 - 1;
    }

    /**
     * {@inheritDoc}
     */
    protected function initializeDoctrineTypeMappings()
    {
        $this->doctrineTypeMapping = [
            // Vertica has only 64-bit integer, but we will treat al ass integer except bigint
            'bigint'            => 'bigint',
            'integer'           => 'integer',
            'int'               => 'integer',
            'int8'              => 'integer',
            'smallint'          => 'integer',
            'tinyint'           => 'integer',

            'boolean'           => 'boolean',

            'varchar'           => 'string',
            'character varying' => 'string',
            'char'              => 'string',
            'character'         => 'string',

            // custom type, Vertica has only varchar, but we will treat bi varchars (4k+) as text
            'text'              => 'text',

            'date'              => 'date',
            'datetime'          => 'datetime',
            'smalldatetime'     => 'datetime',
            'timestamp'         => 'datetime',
            'timestamptz'       => 'datetimetz',
            'time'              => 'time',
            'timetz'            => 'time',

            'float'             => 'float',
            'float8'            => 'float',
            'double precision'  => 'float',
            'real'              => 'float',

            'decimal'           => 'decimal',
            'money'             => 'decimal',
            'numeric'           => 'decimal',
            'number'            => 'decimal',

            'binary'            => 'blob',
            'varbinary'         => 'blob',
            'bytea'             => 'blob',
            'raw'               => 'blob'
        ];
    }
}
