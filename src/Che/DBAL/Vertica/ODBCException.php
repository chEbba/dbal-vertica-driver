<?php
/*
 * Copyright (c)
 * Kirill chEbba Chebunin <iam@chebba.org>
 *
 * This source file is subject to the MIT license that is bundled
 * with this package in the file LICENSE.
 */

namespace Che\DBAL\Vertica;

/**
 * Base exception class for ODBC errors
 *
 * @author Kirill chEbba Chebunin <iam@chebba.org>
 * @license http://www.opensource.org/licenses/mit-license.php MIT
 */
class ODBCException extends \RuntimeException
{
    private $sqlState;

    public function __construct($message = '', $sqlState = 0, \Exception $previous = null)
    {
        $this->sqlState = $sqlState;

        parent::__construct(sprintf('[%s]%s', $this->sqlState, $message), 0, $previous);
    }

    /**
     * @return null
     */
    public function getSqlState()
    {
        return $this->sqlState;
    }

    public static function fromConnection($dbh)
    {
        return new self(odbc_errormsg($dbh), odbc_error($dbh));
    }
}
