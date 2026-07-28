<?php
date_default_timezone_set('America/New_York');
require_once("connection2.php");

$removeAmt = 0;
$removePct = 0.8; // 0.8% per list

// latest servers 2026-07-03:
// ('frozenclass', 'megaspot', 'freshform', 'multistand', 'crystalbin', 'notebubble', 'fairgreen', 'futurehut', 'homeparade', 'titlecamp', 'oceandawn')

$jobSql = "SELECT DISTINCT c.pigeon_mail, c.data_group
			FROM rm_inactive_cron c
			INNER JOIN growth_plan g ON c.cron_id = g.cron_id 
			WHERE c.pigeon_mail in ('frozenclass', 'megaspot', 'freshform', 'multistand', 'crystalbin', 'notebubble', 'fairgreen', 'futurehut', 'homeparade', 'titlecamp', 'oceandawn', 'boostwell', 'chrometime', 'capitalpin', 'melodyhill', 'cyclonelead')
			AND g.current_stage = 3";
// INNER JOIN rm_inactive_cron_day d on c.cron_id = d.cron_id
// AND d.execution_day = (DAYOFWEEK(NOW()) % 7) + 1
$jobRes = $conn->query($jobSql);

if (!$jobRes) {
	die("Failed to fetch jobSql: " . $conn->error . "\n");
}

if ($jobRes->num_rows === 0) {
	die("No job record. Nothing to process.\n");
}

$serverList = [];

while ($row = $jobRes->fetch_assoc()) {
	$serverList[$row['pigeon_mail']][] = $row['data_group'];
}
$conn->close();

$maxChildren = 4;
$children = [];

// $serverList = ["boostwell" => ["Yahoo_New_Train_Waipin_test"]];
echo "Job Start\n\n";

foreach ($serverList as $server => $groups) {
	// wait until there's available worker
	while (count($children) >= $maxChildren) {
		$finishedPid = pcntl_wait($status);

		if ($finishedPid > 0) {
			unset($children[$finishedPid]);
		}
	}

	$pid = pcntl_fork();

	if ($pid == -1) {
		die("Unable to fork\n");
	} elseif ($pid == 0) {
		// child process
		processJob($server, $groups, $removePct);
		exit(0); // must exit, never let child fall through to parent loop
	} else {
		// parent process
		$children[$pid] = true;
		echo "[".date('H:i:s')."] Parent: child $pid created for {$server}.\n";
	}
}

// wait remaining children
while (count($children) > 0) {
	$finishedPid = pcntl_wait($status);

	if ($finishedPid > 0) {
		unset($children[$finishedPid]);
		echo "Parent: Child $finishedPid finished.\n";
	}
}

echo "Job End\n";

function connectPigeonMail($pgMail) {
	$host = "$pgMail.net";
	$username = $pgMail;
	$password = "geekgeek50509";
	$db = "pigeon_mail";

	$pgConn = new mysqli($host, $username, $password, $db);
	if ($pgConn->connect_error) {
		throw new Exception("Failed to connect to $pgMail: " . $pgConn->connect_error . "\n");
	}

	return $pgConn;
}

function getList($ext, $group) {
	$safeGroup = $ext->real_escape_string($group);
	$sql = "SELECT list_id, list_name FROM list WHERE list_group = '$safeGroup'";
	$res = $ext->query($sql);

	$arr = [];
	while ($row = $res->fetch_assoc()) {
		$arr[] = $row;
	}

	return $arr;
}

function checkSending($ext) {
	$sqlInProgJob = "SELECT message_id FROM `message` WHERE message_status = 'inprogress'";
	$progResult = $ext->query($sqlInProgJob);

	if ($progResult && $progResult->num_rows > 0) {
		return true;
	}

	$sqlSendJob = "SELECT job_id FROM `message_job` WHERE job_status = 'sending'";
	$jobResult = $ext->query($sqlSendJob);

	if ($jobResult && $jobResult->num_rows > 0) {
		return true;
	}

	return false;
}

function processJob($server, $groups, $removePct) {
	require 'connection2.php';
	$startTimestamp = time();
	$startDateTime = date('Y-m-d H:i:s', $startTimestamp);
	echo "Start: $startDateTime\n";
	echo "Processing on server: $server\n";

	try {
		$extConn = connectPigeonMail($server);
	} catch (Exception $e) {
		echo "Skipping server $server: " . $e->getMessage();
		return;
	}

	// check if any sending in progress
	if (checkSending($extConn)) {
		echo "Sending in progress on $server.\n";
		$extConn->close();
		return;
	}

	if ($server === "frozenclass") {
		$createTbl = "CREATE TABLE IF NOT EXISTS `wp_list` ( `email` varchar(255), KEY `email` (`email`) ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;";
		echo "createTbl: $createTbl\n";
		$createRes = $extConn->query($createTbl);

		if (!$createRes) {
			echo "createRes failed on $server: " . $extConn->error . "\n";
			$extConn->close();
			return;
		}
	}

	foreach ($groups as $group) {
		echo "Processing group: $group\n";

		$lists = getList($extConn, $group);

		if (empty($lists)) {
			echo "No lists found for group: $group\n";
			continue;
		}

		foreach ($lists as $list) {
			$targetTable = $list['list_name'];
			echo "Processing list: $targetTable\n";		

			$listSql = "SELECT COUNT(distinct(l.email)) as list_size, ceiling(count(distinct(l.email)) * $removePct / 100) as amt_to_remove
				FROM `$targetTable` l
				WHERE l.active = 1";
			// echo $listSql . "\n";
			$listRes = $extConn->query($listSql);

			if (!$listRes) {
				echo "listSql failed on $targetTable: " . $extConn->error . "\n";
				continue;
			}

			$removeAmt = (int) $listRes->fetch_assoc()['amt_to_remove'];

			echo "$targetTable: " . $removeAmt . " email(s) to remove.\n";

			if ($removeAmt > 0) {
				if ($server === "frozenclass") {
					$trunTable = "TRUNCATE TABLE `wp_list`";
					$trunRes = $extConn->query($trunTable);
					if (!$trunRes) {
						echo "Truncate failed on wp_list: " . $extConn->error . "\n";
						continue;
					}

					$insData = "INSERT INTO `wp_list` (email) 
						SELECT DISTINCT(l.email) as EMAIL FROM `$targetTable` l
						left join `user_click` uc ON l.email  = uc.click_email
						WHERE l.active = 1
						and uc.user_agent ='Mozilla/5.0' 
						and uc.click_date < DATE_SUB(NOW(), INTERVAL 3 DAY) and click_date > DATE_SUB(NOW(), INTERVAL 50 DAY)  
						and uc.click_email not in (SELECT click_email FROM `user_click` where link_code in (select link_code from link where link_category like 'click') and click_date  > DATE_SUB(NOW(), INTERVAL 30 DAY))
						and uc.click_email not in (select click_email from user_click where click_date > DATE_SUB(NOW(), INTERVAL 60 DAY) and user_agent like 'YahooMailProxy%')
						and uc.click_email not in (select click_email from user_click_duplicate where click_date > DATE_SUB(NOW(), INTERVAL 15 DAY))";
					$insRes = $extConn->query($insData);

					if (!$insRes) {
						echo "Insert wp_list failed: " . $extConn->error . "\n";
						continue;
					} else {
						echo "Insert wp_list: " . $extConn->affected_rows . " row(s).\n";
					}

					// grab fake openers
					$sel_email_sql = "SELECT distinct(email) as EMAIL
						FROM `wp_list` l
						WHERE email not in (SELECT email FROM `bot_update_log` )
						and email not in (SELECT seed_email FROM `seed_info` )
						and email not in (SELECT log_email FROM `smtp_code_hard_bounce` )
						and email not in (SELECT email_email FROM `unsub_list` )
						ORDER BY RAND()
						LIMIT 0, $removeAmt";
				} else {
					// grab fake openers
					$sel_email_sql = "SELECT distinct(l.email) as EMAIL
						FROM `$targetTable` l
						left join `user_click` uc ON l.email  = uc.click_email
						WHERE l.active = 1
						and uc.click_email not in (SELECT email FROM `bot_update_log` )
						and uc.click_email not in (SELECT seed_email FROM `seed_info` )
						and uc.click_email not in (SELECT log_email FROM `smtp_code_hard_bounce` )
						and uc.click_email not in (SELECT email_email FROM `unsub_list` )
						and uc.user_agent ='Mozilla/5.0' 
						and uc.click_date < DATE_SUB(NOW(), INTERVAL 3 DAY) and click_date > DATE_SUB(NOW(), INTERVAL 50 DAY)  
						and uc.click_email not in (SELECT click_email FROM `user_click` where link_code in (select link_code from link where link_category like 'click') and click_date  > DATE_SUB(NOW(), INTERVAL 30 DAY))
						and uc.click_email not in (select click_email from user_click where click_date > DATE_SUB(NOW(), INTERVAL 60 DAY) and user_agent like 'YahooMailProxy%')
						and uc.click_email not in (select click_email from user_click_duplicate where click_date > DATE_SUB(NOW(), INTERVAL 15 DAY))
						ORDER BY RAND()
						LIMIT 0, $removeAmt";
				}
				$emailRes = $extConn->query($sel_email_sql);

				if (!$emailRes) {
					echo "Query failed on $targetTable: " . $extConn->error . "\n";
					continue;
				}

				if ($emailRes->num_rows === 0) {
					echo "No emails found in $targetTable - skipping.\n";
					continue;
				}

				$emails = [];
				while ($eRow = $emailRes->fetch_assoc()) {
					$emails[] = $eRow['EMAIL'];
				}
				echo "Found " . count($emails) . " email(s) to process.\n";

				// batch insert to pool
				$insertedCount = 0;
				$skippedCount = 0;
				$valuesInsert = [];

				foreach ($emails as $email) {
					$safeEmail = $conn->real_escape_string($email);
					$safeServer = $conn->real_escape_string($server);
					list($localPart, $domain) = explode('@', $email, 2);
					$safeDomain = $conn->real_escape_string($domain);
					// $safeList = $conn->real_escape_string($targetTable);
					$valuesInsert[] = "('$safeEmail', '$safeDomain', '$safeServer', current_timestamp)";
				}

				if (!empty($valuesInsert)) {
					$batchInsertSql = "INSERT IGNORE INTO `the_big_pool_openers_nonclickers` (pool_email, pool_domain, from_server, pool_timestamp)
										VALUES " . implode(', ', $valuesInsert);
					// echo $batchInsertSql . "\n";
					$insertRes = $conn->query($batchInsertSql);

					if ($insertRes) {
						$insertedCount = $conn->affected_rows;
						$skippedCount  = count($emails) - $insertedCount;
						echo "Inserted: $insertedCount | Skipped (duplicate): $skippedCount\n";
					} else {
						echo "Batch insert failed: " . $conn->error . "\n";
						continue;
					}
				}

				// delete emails from list
				$safeEmailsForDelete = [];
				foreach ($emails as $email) {
					$safeEmailsForDelete[] = "'" . $extConn->real_escape_string($email) . "'";
				}

				$inClause = implode(', ', $safeEmailsForDelete);
				$deleteSql = "DELETE FROM `$targetTable` WHERE email IN ($inClause)";
				// echo $deleteSql . "\n";
				$deleteRes = $extConn->query($deleteSql);

				if ($deleteRes) {
					echo "Deleted " . $extConn->affected_rows . " row(s) from $targetTable.\n";
				} else {
					echo "Delete failed on $targetTable: " . $extConn->error . "\n";
				}
			} else {
				echo "$targetTable: No email(s) to remove ($removeAmt).\n"; 
			}
		}
	}

	if ($server === "frozenclass") {
		$dropTable = "DROP TABLE IF EXISTS `wp_list`";
		echo "dropTable: $dropTable\n";
		$extConn->query($dropTable);
	}

	$extConn->close();
	echo "Done with server: $server\n";
	$endTimestamp = time();
	$endDateTime = date('Y-m-d H:i:s', $endTimestamp);
	echo "End $server: $endDateTime\n\n";
}
?>
