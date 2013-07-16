<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica\Tests;

use Che\DBAL\Vertica\ODBCConnection;
use Che\DBAL\Vertica\ODBCException;
use PHPUnit_Framework_TestCase as TestCase;

/**
 * Functional test for ODBCConnection & ODBCStatement
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class ODBCConnectionFunctionalTest extends TestCase
{
    /**
     * @var ODBCConnection
     */
    private $conn;

    /**
     * Setup connection and test tables
     */
    protected function setUp()
    {
        $this->conn = $this->createConnection();
        $this->conn->exec("DROP SCHEMA IF EXISTS doctrine CASCADE");
        $this->conn->exec("CREATE SCHEMA doctrine");
        $this->conn->exec("CREATE TABLE doctrine.test (id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)");
        $this->conn->exec("INSERT INTO doctrine.test (id, name) VALUES (1, 'foo')");
    }

    protected function createConnection(array $parameters = [], array $options = [])
    {
        $parameters = array_merge(
            [
                'dsn' => $dsn = sprintf('Driver=Vertica;Servername=%s;Port=%s;Database=%s', $GLOBALS['db_host'], $GLOBALS['db_port'], $GLOBALS['db_name']),
                'username' => $GLOBALS['db_username'],
                'password' => $GLOBALS['db_password']

            ],
            $parameters
        );
        return new ODBCConnection($parameters['dsn'], $parameters['username'] , $parameters['password'], $options);
    }

    protected function insertTestRow()
    {
        $this->conn->exec("INSERT INTO doctrine.test (id, name) VALUES (2, 'bar')");
    }

    public function testConnectionError()
    {
        try {
            $this->createConnection(['username' => 'foo']);
        } catch (ODBCException $e) {
            return;
        }

        $this->fail('Connection exception was not thrown');
    }

    public function testColumnCount()
    {
        $stmt = $this->conn->query('SELECT id, name FROM doctrine.test');
        $this->assertEquals(2, $stmt->columnCount());
    }

    public function testRowCount()
    {
        $stmt = $this->conn->query('SELECT id, name FROM doctrine.test');
        $this->assertEquals(1, $stmt->rowCount());
    }

    /**
     * @dataProvider fetchResults
     *
     * @param $fetchMode
     * @param $results
     */
    public function testFetch($fetchMode, $results)
    {
        $this->conn->exec("INSERT INTO doctrine.test (id, name) VALUES (2, 'bar')");
        $stmt = $this->conn->prepare('SELECT id, name FROM doctrine.test');
        $stmt->execute();
        $this->assertEquals($results[0], $stmt->fetch($fetchMode));
        $this->assertEquals($results[1], $stmt->fetch($fetchMode));
        $this->assertEquals(null, $stmt->fetch($fetchMode));
    }

    /**
     * @dataProvider fetchResults
     *
     * @param $fetchMode
     * @param $results
     */
    public function testFetchAll($fetchMode, $results)
    {
        $this->insertTestRow();
        $stmt = $this->conn->query('SELECT id, name FROM doctrine.test');

        $this->assertEquals($results, $stmt->fetchAll($fetchMode));
    }

    public function fetchResults()
    {
        return [
            [
                \PDO::FETCH_ASSOC,
                [
                    ['id' => 1, 'name' => 'foo'],
                    ['id' => 2, 'name' => 'bar']
                ]
            ],
            [
                \PDO::FETCH_NUM,
                [
                    [1, 'foo'],
                    [2, 'bar']
                ]
            ],
            [
                \PDO::FETCH_BOTH,
                [
                    [1, 'foo', 'id' => 1, 'name' => 'foo'],
                    [2, 'bar', 'id' => 2, 'name' => 'bar']
                ]
            ]
        ];
    }

    public function testFetchColumn()
    {
        $this->insertTestRow();
        $stmt = $this->conn->query('SELECT id, name FROM doctrine.test');

        $this->assertEquals('1', $stmt->fetchColumn());
        $this->assertEquals('bar', $stmt->fetchColumn(1));
    }

    /**
     * @dataProvider queries
     *
     * @param $sql
     * @param array $params
     * @param $data
     */
    public function testPrepared($sql, array $params = [], array $data = [])
    {
        $stmt = $this->conn->prepare($sql);
        $stmt->execute($params);
        $this->assertEquals($data, $stmt->fetchAll(\PDO::FETCH_ASSOC));
    }

    public function queries()
    {
        return [
            [
                'SELECT id, name FROM doctrine.test',
                [],
                [
                    [
                        'id' => 1,
                        'name' => 'foo'
                    ]
                ]
            ],
            [
                'SELECT id, name FROM doctrine.test WHERE id=:id',
                [':id' => 1],
                [
                    ['id' => 1, 'name' => 'foo']
                ]
            ],
            [
                'SELECT id, name FROM doctrine.test WHERE id=?',
                [1],
                [
                    ['id' => 1, 'name' => 'foo']
                ]
            ],
            [
                'SELECT id, name FROM doctrine.test WHERE id=:id AND name=:name',
                [':id' => 1, ':name' => 'foo'],
                [
                    ['id' => 1, 'name' => 'foo']
                ]
            ],
            [
                'SELECT id, name FROM doctrine.test WHERE id=? AND name=?',
                [1, 'foo'],
                [
                    ['id' => 1, 'name' => 'foo']
                ]
            ]
        ];
    }

    public function testPositionalBind()
    {
        $stmt = $this->conn->prepare('SELECT id, name FROM doctrine.test WHERE id=? AND name=?');
        $id = 1;
        $name = 'foo';
        $stmt->bindParam(1, $id);
        $stmt->bindParam(2, $name);
        $stmt->execute();
        $this->assertEquals(
            [
                [
                    'id' => 1,
                    'name' => 'foo'
                ]
            ],
            $stmt->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testNamedBind()
    {
        $stmt = $this->conn->prepare('SELECT id, name FROM doctrine.test WHERE id=:id AND name=:name');
        $id = 1;
        $name = 'foo';
        $stmt->bindParam(':id', $id);
        $stmt->bindParam(':name', $name);
        $stmt->execute();
        $this->assertEquals(
            [
                [
                    'id' => 1,
                    'name' => 'foo'
                ]
            ],
            $stmt->fetchAll(\PDO::FETCH_ASSOC)
        );
    }

    public function testTraversable()
    {
        $this->insertTestRow();
        $stmt = $this->conn->query('SELECT id, name FROM doctrine.test');
        $stmt->setFetchMode(\PDO::FETCH_ASSOC);
        $results = [
            ['id' => 1, 'name' => 'foo'],
            ['id' => 2, 'name' => 'bar']
        ];
        foreach ($stmt as $i => $row) {
            $this->assertEquals($row, $results[$i]);
        }
    }

    public function testMultipleExec()
    {
        $this->conn = $this->createConnection([], [ODBCConnection::OPTION_EMULATE_MULTIPLE_EXEC => true]);
        $this->insertTestRow();
        $stmt = $this->conn->prepare('SELECT id, name FROM doctrine.test WHERE id=?');
        $stmt->execute([1]);
        $this->assertEquals([1, 'foo'], $stmt->fetch(\PDO::FETCH_NUM));
        $stmt->execute([2]);
        $this->assertEquals([2, 'bar'], $stmt->fetch(\PDO::FETCH_NUM));
    }

    public function testCommit()
    {
        $this->conn->beginTransaction();
        $this->insertTestRow();
        $this->conn->commit();
        $this->assertEquals(2, $this->conn->query('SELECT COUNT(*) FROM doctrine.test')->fetchColumn());
    }

    public function testRollback()
    {
        $this->conn->beginTransaction();
        $this->insertTestRow();
        $this->conn->rollBack();
        $this->assertEquals(1, $this->conn->query('SELECT COUNT(*) FROM doctrine.test')->fetchColumn());
    }

    public function testMixedParametersException()
    {
        try {
            $this->conn->prepare('SELECT id, name FROM doctrine.test WHERE id=? AND name=:name');
        } catch (ODBCException $e) {
            return;
        }

        $this->fail('Exception on mixed parameter types was not thrown');
    }

    public function testUnknownParameterException()
    {
        $stmt = $this->conn->prepare('SELECT id, name FROM doctrine.test WHERE id=?');
        try {
            $stmt->bindValue(2, 1);
        } catch (ODBCException $e) {
            return;
        }

        $this->fail('Exception on unknown parameter was not thrown');
    }

    public function testExecuteParameterCountException()
    {
        $stmt = $this->conn->prepare('SELECT id, name FROM doctrine.test WHERE id=?');
        try {
            $stmt->execute();
        } catch (ODBCException $e) {
            return;
        }

        $this->fail('Exception on execute error was not thrown');
    }
}
