<?php

/*

Copyright (c) 2012, Kimo Rosenbaum and contributors
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

*/

/**
 * APIProducerV2DriverMySQL
 * @author Kimo Rosenbaum <kimor79@yahoo.com>
 * @version $Id$
 * @package APIProducerV2DriverMySQL
 */

include __DIR__ . '/../classes/driver.php';

class APIProducerV2DriverMySQL extends APIProducerV2Driver{

	private $mysql;
	protected $prefix = '';
	protected $query_on_error = false;

	public function __construct($slave_okay = false, $config = array()) {
		parent::__construct($slave_okay, $config);

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
		parent::__deconstruct();
		$this->mysql->close();
	}

	/**
	 * Turn the parameters into SQL for use by select()
	 * @return array order, group, 
	 */
	protected function applyParameters() {
		$output = array();
		$limit = array();

		if($this->getParameter('numResults')) {
			$output['limit'][] = $this->getParameter('numResults');
		}

		if($this->getParameter('startIndex')) {
			array_unshift($output['limit'],
				$this->getParameter('numResults'));
		}

		if(!is_null($this->getParameter('sortField'))) {
			$output['order'] =
				'`' . $this->getParameter('sortField') . '`';

			if(!is_null($this->getParameter('sortDir'))) {
				$output['order'] .= ' ' .
					$this->getParameter('sortDir');
			}
		}

		if(!empty($limit)) {
			$output['limit'] = $limit;
		}

		return $output;
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
	 * Parse a query built using buildQuery
	 * @param array $search
	 * @return array binds, values, where
	 */
	protected function parseQuery($search) {
		$eqs = array();
		$output = array(
			'binds' => '',
			'values' => array(),
			'where' => '',
		);
		$res = array();
		$where = array();

		while(list($field, $types) = each($search)) {
			foreach($types as $type => $values) {
				switch($type) {
				// Too many indents
				case 'ge':
					$output['binds'] .= 's';
					$output['values'][] = $values;
					$where[] = '`' . $field .'` >= ?';
					break;
				case 'gt':
					$output['binds'] .= 's';
					$output['values'][] = $values;
					$where[] = '`' . $field .'` > ?';
					break;
				case 'le':
					$output['binds'] .= 's';
					$output['values'][] = $values;
					$where[] = '`' . $field .'` <= ?';
					break;
				case 'lt':
					$output['binds'] .= 's';
					$output['values'][] = $values;
					$where[] = '`' . $field .'` < ?';
					break;
				case 're':
					$res = array();
					while(list($re, $value) =
							each($values)) {
						$output['binds'] .= 's';
						$output['values'][] = $value;
						$res[] = '`' . $field .
							'` REGEXP ?';
					}
					reset($values);

					$where[] = '(' .
						implode(' OR ', $res) . ')';
					break;
				default:
					$eqs = array();
					while(list($eq, $value) =
							each($values)) {
						$output['binds'] .= 's';
						$output['values'][] = $value;
						$eqs[] = '?';
					}
					reset($values);

					$where[] = sprintf("`%s` IN (%s)",
						$field, implode(', ', $eqs));
					break;
				}
			}
		}
		reset($search);

		$output['where'] = implode(' AND ', $where);

		return $output;
	}

	/**
	 * Prep fields
	 * @param array $fields
	 * @param array $input
	 * @return array binds, sets, values
	 */
	protected function prepFields($fields, $input = array()) {
		$binds = '';
		$sets = array();
		$values = array();

		foreach ($fields as $field => $mfield) {
			if(array_key_exists($field, $input)) {
				$binds .= 's';
				$sets[] = $mfield . ' = ?';
				$values[] = $input[$field];
			}
		}

		return array($binds, $sets, $values);
	}

	/**
	 * Prep fields for INSERT .. VALUES
	 * @param array $fields
	 * @param array $input
	 * @return array binds, cols, sets, values
	 */
	protected function prepFieldsMulti($fields, $input = array()) {
		$binds = '';
		$cols = array();
		$sets = array();
		$values = array();

		foreach ($fields as $field => $mfield) {
			if(array_key_exists($field, $input)) {
				$binds .= 's';
				$cols[] = $mfield;
				$sets[] = '?';
				$values[] = $input[$field];
			}
		}

		return array($binds, $cols, $sets, $values);
	}

	/**
	 * Perform a read-only query
	 * @param string $query
	 * @param array $binds
	 * @param array $values
	 * @return mixed records or false
	 */
	protected function queryRead($query, $binds, $values) {
		$this->error = '';

		$refs = array();

		$st = $this->mysql->prepare($query);
		if(!$st) {
			$this->error = $this->mysql->error;
			if($this->query_on_error) {
				$this->error .= ': ' . $query;
			}
			return false;
		}

		if(!empty($binds)) {
			while(list($value, $junk) = each($values)) {
				$refs[] = &$values[$value];
			}

			array_unshift($refs, $binds);

			if(!call_user_func_array(array($st, 'bind_param'),
					$refs)) {
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
	 * @param array $values
	 * @param bool $last_id True to return insert_id
	 * @return mixed affected rows (or id)  or false
	 */
	protected function queryWrite($query, $binds, $values,
			$last_id = false) {
		$this->error = '';

		$refs = array();

		$st = $this->mysql->prepare($query);
		if(!$st) {
			$this->error = $this->mysql->error;
			if($this->query_on_error) {
				$this->error .= ': ' . $query;
			}
			return false;
		}

		while(list($value, $junk) = each($values)) {
			$refs[] = &$values[$value];
		}

		array_unshift($refs, $binds);

		if(call_user_func_array(array($st, 'bind_param'), $refs)) {
			if($st->execute()) {
				if($last_id) {
					return $this->mysql->insert_id;
				}

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
		$binds = '';
		$query = 'SELECT ';
		$values = array();

		$query .= (array_key_exists('select', $statements)) ?
			$statements['select'] : '*';

		if(array_key_exists('from', $statements)) {
			$query .= ' FROM ' . $statements['from'];
		}

		if(array_key_exists('where', $statements) &&
				($statements['where'])) {
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
			$limit_one = 0;

			if(count($statements['limit']) == 1) {
				$limit_two = $statements['limit'][0];
			} else {
				$limit_one = $statements['limit'][0];
				$limit_two = $statements['limit'][1];
			}

			$query .= sprintf(" LIMIT %s, %s",
				$limit_one, $limit_two);
		}

		if(array_key_exists('procedure', $statements)) {
			$query .= ' PROCEDURE ' . $statements['procedure'];
		}

		if(array_key_exists('_binds', $statements)) {
			$binds = $statements['_binds'];
		}

		if(array_key_exists('_values', $statements)) {
			$values = $statements['_values'];
		}

		$data = $this->queryRead($query, $binds, $values);
		if(is_array($data)) {
			$records = array();

			while(list($junk, $record) = each($data)) {
				if(!empty($statements['_one'])) {
					return $record;
				}

				if(!empty($statements['_single'])) {
					$records[] =
						$record[$statements['_single']];
				} else {
					$records[] = $record;
				}
			}
			reset($data);

			return $records;
		}

		return false;
	}
}

?>
