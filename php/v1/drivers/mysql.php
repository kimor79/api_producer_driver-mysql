<?php

/**

Copyright (c) 2011-2012, Kimo Rosenbaum and contributors
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the owner nor the names of its contributors
      may be used to endorse or promote products derived from this
      software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**/

/**
 * ApiProducerDriverMySQL
 * @author Kimo Rosenbaum <kimor79@yahoo.com>
 * @version $Id$
 * @package ApiProducerDriverMySQL
 */

class ApiProducerDriverMySQL {

	protected $config = array();
	protected $count = 0;
	protected $error = '';
	private $mysql;
	private $prefix = '';
	protected $query_on_error = false;
	protected $slave_okay = false;

	public function __construct($slave_okay = false, $config = array()) {
		$this->config = $config;
		$this->slave_okay = $slave_okay;

		$database = $this->getConfig('database', '');
		$host = $this->getConfig('host',
			ini_get('mysqli.default_host'));
		$password = $this->getConfig('password',
			ini_get('mysqli.default_pw'));
		$port = $this->getConfig('port',
			ini_get('mysqli.default_port'));
		$socket = $this->getConfig('socket',
			ini_get('mysqli.default_socket'));
		$user = $this->getConfig('user',
			ini_get('mysqli.default_user'));

		$this->prefix = $this->getConfig('prefix', '');
		$this->query_on_error = $this->getConfig('query_on_error',
			false);

		$this->mysql = @new mysqli($host, $user, $password, $database,
			$port, $socket);

		if(mysqli_connect_errno()) {
			throw new Exception(mysqli_connect_error());
		}
	}

	public function __deconstruct() {
		$this->mysql->close();
	}

	/**
	 * Return the total number of records from a query
	 * @return int
	 */
	public function count() {
		return (int) $this->count;
	}

	/**
	 * Return error (if any) from most recent query
	 * @return string
	 */
	public function error() {
		return $this->error;
	}

	/**
	 * Get the list of columns for a table
	 * @param string $table
	 * @return mixed array or false
	 */
	protected function getColumns($table) {
		$record = $this->select(array(
			'_one' => true,
			'from' => sprintf("`%s%s`", $this->prefix, $table),
			'limit' => array(0, 1),
		));

		if(is_array($record)) {
			return array_keys($record);
		}

		return false;
	}

	/**
	 * Get a config value
	 * @param string $key
	 * @param string $default
	 * @return string
	 */
	protected function getConfig($key = '', $default = '') {
		$type = 'rw_' . $key;
		if($this->slave_okay) {
			$type = 'ro_' . $key;
		}

		if(array_key_exists($type, $this->config)) {
			return $this->config[$type];
		}

		if(array_key_exists($key, $this->config)) {
			return $this->config[$key];
		}

		return $default;
	}

	/**
	 * Perform a read-only query
	 * @param string $query
	 * @param array $binds
	 * @return mixed records or false
	 */
	protected function queryRead($query, $binds) {
		$this->error = '';

		$st = $this->mysql->prepare($query);
		if(!$st) {
			$this->error = $this->mysql->error;
			if($this->query_on_error) {
				$this->error .= ': ' . $query;
			}
			return false;
		}

		if(!empty($binds)) {
			if(!call_user_func_array(array($st, 'bind_param'),
					$binds)) {
				if($st->errno) {
					$this->error = $st->error;
				}

				if($this->query_on_error) {
					$this->error .= ': ' . $query;
				}

				$st->close();
				return false;
			}
		}

		if(!$st->execute()) {
			if($st->errno) {
				$this->error = $st->error;
			}

			if($this->query_on_error) {
				$this->error .= ': ' . $query;
			}

			$st->close();
			return false;
		}

		if(!$st->store_result()) {
			if($st->errno) {
				$this->error = $st->error;
			}

			if($this->query_on_error) {
				$this->error .= ': ' . $query;
			}

			$st->close();
			return false;
		}

		$result = $st->result_metadata();
		if(!$result) {
			if($st->errno) {
				$this->error = $st->error;
			}

			if($this->query_on_error) {
				$this->error .= ': ' . $query;
			}

			$st->close();
			return false;
		}

		$columns = array();
		foreach($result->fetch_fields() as $field) {
			$columns[] = &$fields[$field->name];
		}

		if(call_user_func_array(array($st, 'bind_result'), $columns)) {
			$records = array();
			while($st->fetch()) {
				$details = array();
				foreach($fields as $field => $value) {
					$details[$field] = $value;
				}

				$records[] = $details;
			}

			$st->close();
			return $records;
		}

		if($st->errno) {
			$this->error = $st->error;
		}

		if($this->query_on_error) {
			$this->error .= ': ' . $query;
		}

		$st->close();
		return false;
	}

	/**
	 * Perform a write query
	 * @param string $query
	 * @param array $binds
	 * @return mixed affected rows or false
	 */
	protected function queryWrite($query, $binds) {
		$this->error = '';

		$st = $this->mysql->prepare($query);
		if(!$st) {
			$this->error = $this->mysql->error;
			if($this->query_on_error) {
				$this->error .= ': ' . $query;
			}
			return false;
		}

		if(call_user_func_array(array($st, 'bind_param'), $binds)) {
			if($st->execute()) {
				if(is_numeric($st->affected_rows)) {
					$rows = $st->affected_rows;

					$st->close();
					return $rows;
				}
			}
		}

		$this->error = '';
		if($st->errno) {
			$this->error = $st->error;
		}

		if($this->query_on_error) {
			$this->error .= ': ' . $query;
		}

		$st->close();
		return false;
	}

	/**
	 * Build and run a select statement
	 * @param array $statements
	 * @return mixed array of records or false
	 */
	protected function select($statements = array()) {
		$query = 'SELECT ';
		$refs = array();

		$query .= (array_key_exists('select', $statements)) ?
			$statements['select'] : '*';

		if(array_key_exists('from', $statements)) {
			$query .= ' FROM ' . $statements['from'];
		}

		if(array_key_exists('where', $statements)) {
			$query .= ' WHERE ' . $statements['where'];
		}

		if(array_key_exists('group', $statements)) {
			$query .= ' GROUP BY ' . $statements['group'];
		}

		if(array_key_exists('having', $statements)) {
			$query .= ' HAVING ' . $statements['having'];
		}

		if(array_key_exists('order', $statements)) {
			$query .= ' ORDER BY ' . $statements['order'];
		}

		if(array_key_exists('limit', $statements)) {
			if(!$statements['limit'][0]) {
				$statements['limit'][0] = 0;
			}

			$query .= sprintf(" LIMIT %s, %s",
				$statements['limit'][0],
				(array_key_exists(1, $statements['limit'])) ?
					$statements['limit'][1] :
					'18446744073709551615');
		}

		if(array_key_exists('procedure', $statements)) {
			$query .= ' PROCEDURE ' . $statements['procedure'];
		}

		if(array_key_exists('_refs', $statements)) {
			$refs = $statements['_refs'];
		}

		if(array_key_exists('_binds', $statements)) {
			array_unshift($refs, $statements['_binds']);
		}

		$data = $this->queryRead($query, $refs);
		if(is_array($data)) {
			$records = array();

			while(list($junk, $record) = each($data)) {
				if($statements['_one']) {
					return $record;
				}

				if($statements['_single']) {
					$records[] =
						$record[$statements['_single']];
				} else {
					$records[] = $record;
				}
			}

			return $records;
		}

		return false;
	}

	/**
	 * Set the count
	 * @param mixed the total number
	 */
	protected function setCount($count) {
		if(is_numeric($count)) {
			$this->count = $count;
		} else {
			$this->count = 0;
		}
	}
}

?>
