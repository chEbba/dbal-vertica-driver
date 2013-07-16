<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica;

use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\SQLParserUtils;

/**
 * Statement implementation for ODBC connection
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class ODBCStatement implements \Iterator, Statement
{
    private $dbh;
    private $originalQuery;
    private $query;
    private $sth;
    private $options;
    private $defaultFetchMode = \PDO::FETCH_BOTH;
    private $paramMap;
    private $params;
    private $executed = false;
    private $started = false;
    private $key = -1;
    private $current = null;

    public function __construct($dbh, $query, array $options = [])
    {
        $this->options = $options;
        $this->dbh = $dbh;
        $this->parseQuery($query);
        $this->prepare();
    }

    /**
     * Parses query to replace named parameters with positional
     *
     * @param $query
     */
    protected function parseQuery($query)
    {
        $this->originalQuery = $query;
        $this->query = $query;
        $this->paramMap = [];
        $this->params = [];

        $positions = array_flip(SQLParserUtils::getPlaceholderPositions($query));
        if ($positions) {
            if (SQLParserUtils::getPlaceholderPositions($query, false)) {
                throw new ODBCException('Positional and named parameters can not be mixed');
            }

            // We have only positional parameters so we need only remap keys for 1-based indexes
            $this->paramMap = array_combine(range(1, count($positions)), $positions);

            return;
        }

        $positions = SQLParserUtils::getPlaceholderPositions($query, false);
        if (!$positions) {
            return;
        }

        // Remap name parameters to positional
        $queryLength = strlen($query);
        $queryParts = [$query];
        $i = 0;
        foreach ($positions as $pos => $param) {
            // replace named parameter placeholder with position one
            $this->paramMap[':'.$param] = $i;
            $lastPart = array_pop($queryParts);
            $queryParts[] = substr($lastPart, 0, -1 * ($queryLength - $pos));
            $queryParts[] = substr($lastPart, $pos + strlen($param) + 1);

            $i++;
        }

        $this->query = implode('?', $queryParts);
    }

    /**
     * Prepare parsed query
     *
     * @throws ODBCException
     */
    protected function prepare()
    {
        $this->sth = @odbc_prepare($this->dbh, $this->query);
        if (!$this->sth) {
            throw ODBCException::fromConnection($this->dbh);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function bindValue($param, $value, $type = null)
    {
        $this->bindParam($param, $value, $type);
    }

    /**
     * {@inheritDoc}
     */
    public function bindParam($column, &$variable, $type = null, $length = null)
    {
        if (!isset($this->paramMap[$column])) {
            throw new ODBCException(
                sprintf('Parameter identifier "%s" is not presented in the query "%s"', $column, $this->originalQuery)
            );
        }

        $this->params[$this->paramMap[$column]] = &$variable;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        $this->defaultFetchMode = $fetchMode;
    }

    /**
     * {@inheritDoc}
     */
    public function fetch($fetchMode = null)
    {
        $fetchMode = $fetchMode ?: $this->defaultFetchMode;
        $fetched = odbc_fetch_row($this->sth);
        if (!$fetched) {
            return false;
        }
        $numFields = odbc_num_fields($this->sth);
        $row = [];
        switch ($fetchMode) {
            case \PDO::FETCH_ASSOC:
                for ($i = 1; $i <= $numFields; $i++) {
                    $row[odbc_field_name($this->sth, $i)] = odbc_result($this->sth, $i);
                }
                break;

            case \PDO::FETCH_NUM:
                for ($i = 1; $i <= $numFields; $i++) {
                    $row[] = odbc_result($this->sth, $i);
                }
                break;

            case \PDO::FETCH_BOTH;
                for ($i = 1; $i <= $numFields; $i++) {
                    $value = odbc_result($this->sth, $i);
                    $row[] = $value;
                    $row[odbc_field_name($this->sth, $i)] = $value;
                }
                break;

            default:
                throw new \InvalidArgumentException(sprintf('Unsupported fetch mode "%s"', $fetchMode));
        }

        return $row;
    }

    /**
     * {@inheritDoc}
     */
    public function execute($params = null)
    {
        if (!empty($this->options[ODBCConnection::OPTION_EMULATE_MULTIPLE_EXEC]) && $this->executed) {
            $this->prepare();
            $this->executed = false;
        }

        if ($params) {
            foreach ($params as $pos => $value) {
                if (is_int($pos)) {
                    $pos += 1;
                }
                $this->bindValue($pos, $value);
            }
        }

        if (count($this->params) != count($this->paramMap)) {
            throw new ODBCException(sprintf(
                'Parameter count (%s) does not match prepared placeholder count (%s)',
                count($params), count($this->paramMap)
            ));
        }

        if (!@odbc_execute($this->sth, $this->params)) {
            throw ODBCException::fromConnection($this->dbh);
        }

        $this->executed = true;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function fetchAll($fetchMode = null)
    {
        $rows = [];
        while ($row = $this->fetch($fetchMode)) {
            $rows[] = $row;
        }

        return $rows;
    }

    /**
     * {@inheritDoc}
     */
    public function fetchColumn($columnIndex = 0)
    {
        $fetched = odbc_fetch_row($this->sth);
        if (!$fetched) {
            return false;
        }

        return odbc_result($this->sth, $columnIndex + 1);
    }

    /**
     * {@inheritDoc}
     */
    public function columnCount()
    {
        return odbc_num_fields($this->sth);
    }

    /**
     * {@inheritDoc}
     */
    public function rowCount()
    {
        return odbc_num_rows($this->sth);
    }

    /**
     * {@inheritDoc}
     */
    public function closeCursor()
    {
        return odbc_free_result($this->sth);
    }


    /**
     * {@inheritDoc}
     */
    public function errorCode()
    {
        return odbc_error($this->dbh);
    }

    /**
     * {@inheritDoc}
     */
    public function errorInfo()
    {
        return [
            'code' => odbc_error($this->dbh),
            'message' => odbc_errormsg($this->dbh)
        ];
    }

    /**
     * {@inheritDoc}
     */
    public function rewind()
    {
        if ($this->started) {
            throw new ODBCException('Statement can not be rewound after iteration is started');
        }

        $this->next();
    }

    /**
     * {@inheritDoc}
     */
    public function current()
    {
        return $this->current;
    }

    /**
     * {@inheritDoc}
     */
    public function next()
    {
        if (!$this->executed) {
            $this->execute();
        }
        $this->key++;
        $this->started = true;
        $this->current = $this->fetch();
    }

    /**
     * {@inheritDoc}
     */
    public function key()
    {
        return $this->key >= 0 ? $this->key : null;
    }

    /**
     * {@inheritDoc}
     */
    public function valid()
    {
        return $this->current !== false;
    }
}
