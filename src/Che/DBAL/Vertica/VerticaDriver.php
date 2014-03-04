<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;

/**
 * DBAL Driver for {@link http://www.vertica.com/ Vertica}
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class VerticaDriver implements Driver
{
    /**
     * Attempts to create a connection with the database.
     *
     * @param array  $params        All connection parameters passed by the user.
     *                                  - dsn: ODBC dsn, if provided all other parameters are ignored
     *                                  - driver: ODBC Driver name, default to Vertica
     *                                  - host: server host
     *                                  - port: server port
     *                                  - dbname: database name
     * @param string $username      The username to use when connecting.
     * @param string $password      The password to use when connecting.
     * @param array  $driverOptions The driver options to use when connecting.
     *
     * @return Driver\Connection The database connection.
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = [])
    {
        return new ODBCConnection($this->_constructDsn($params), $username, $password);
    }

    private function _constructDsn(array $params)
    {
        $dsn = '';
        if (!empty($params['dsn'])) {
            $dsn .= $params['dsn'];
        } else {
            $dsn .= 'Driver=' . (!empty($params['odbc_driver']) ? $params['odbc_driver'] : 'VerticaDSN') . ';';
            if (isset($params['host'])) {
                $dsn .= 'Servername=' . $params['host'] . ';';
            }
            if (isset($params['port'])) {
                $dsn .= 'Port=' . $params['port'] . ';';
            }
            if (isset($params['dbname'])) {
                $dsn .= 'Database=' . $params['dbname'] . ';';
            }
        }

        return $dsn;
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabasePlatform()
    {
        return new VerticaPlatform();
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaManager(Connection $conn)
    {
        return new VerticaSchemaManager($conn);
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'vertica';
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabase(Connection $conn)
    {
        $params = $conn->getParams();

        if (isset($params['dbname'])) {
            return $params['dbname'];
        }

        return $conn->query('SELECT CURRENT_DATABASE()')->fetchColumn();
    }
}
