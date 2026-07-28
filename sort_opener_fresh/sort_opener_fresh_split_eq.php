<?php
date_default_timezone_set('America/New_York');
require_once("connection2.php");
$frontPercent = 20;
$backPercent = 20;

$jobSql = "SELECT pigeon_mail, data_group
			FROM rm_inactive_cron
			WHERE pigeon_mail = 'oceandawn'";
$jobRes = $conn->query($jobSql);

if (!$jobRes) {
	die("Failed to fetch jobSql: " . $conn->error . "\n");
}

if ($jobRes->num_rows === 0) {
	die("No job record. Nothing to process.\n");
}

$currentHour = date('H');
$serverList = [];

while ($row = $jobRes->fetch_assoc()) {
	$serverList[$row['pigeon_mail']][] = $row['data_group'];
}

$conn->close();

if (empty($serverList)) {
	die("No job matched current session (hour: $currentHour). Nothing to process.\n");
}

foreach ($serverList as $server => $groups) {
	$startTimestamp = time();
	$startDateTime = date('Y-m-d H:i:s', $startTimestamp);
	echo "Start: $startDateTime\n";
	echo "Processing on server: $server\n";

	try {
		$extConn = connectPigeonMail($server);
	} catch (Exception $e) {
		echo "Skipping server $server: " . $e->getMessage();
		continue;
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
			$listID = $list['list_id'];

			echo "Process sorting list: " . $targetTable . "\n";

			// check opener
			$openerCount = getOpener($extConn, $targetTable);

			if ($openerCount == 0) {
				echo "No opener found for $targetTable, skip.\n\n";
				continue;
			}

			$sortedTable = $targetTable . "_sorted";

			// remove seed
			removeSeed($extConn, $targetTable, $listID);

			// sort native and fresh
			sortOpenerFresh($extConn, $targetTable, $sortedTable, $frontPercent, $backPercent);

			// import seed
			$finalTable = importSeedEmail($extConn, $sortedTable, $listID);

			// swap table: swap finalTable -> targetTable
			swapTable($extConn, $targetTable, $finalTable);

			echo "Done: " . $targetTable . "\n\n";
		}
	}

	// sort email id
	sortTableIdByGroup($extConn, $groups);

	$extConn->close();
	$endTimestamp = time();
	$endDateTime = date('Y-m-d H:i:s', $endTimestamp);
	echo "End: $endDateTime\n\n";
}

function sortOpenerFresh($ext, $list, $sortedList, $frontPct, $backPct) {
	// total emails
	$sql  = "SELECT COUNT(*) AS total_email FROM `$list`";
	$totalRes = $ext->query($sql);
	$totalRow = $totalRes->fetch_assoc();
	$total = $totalRow['total_email'];

	$frontLimit = floor($total * ($frontPct / 100)); // front opener
	$backLimit  = floor($total * ($backPct / 100)); // back opener

	// get yahoo opener
	$opener = getOpener($ext, $list, true);
	$openerCount = count($opener);

	// check if enough openers for both front and back
	if ($openerCount < ($frontLimit + $backLimit)) {
		echo "not enough openers ($openerCount) for front ($frontLimit) + back ($backLimit). split equally.\n";
		$frontLimit = ceil($openerCount / 2);
		$backLimit  = floor($openerCount / 2);
		echo "new split - front opener count: $frontLimit, back opener count: $backLimit\n";
	}

	$frontNative = array_slice($opener, 0, $frontLimit);
	$remaining = array_slice($opener, count($frontNative));
	$backNative = array_slice($remaining, 0, $backLimit);
	$midNative = array_slice($remaining, count($backNative));

	// add all opener (active=1)
	$usedNative = array_merge($frontNative, $backNative);

	if (!empty($usedNative)) {
		$escapedNative = [];
		foreach ($usedNative as $row) {
			$escapedNative[] = mysqli_real_escape_string($ext, $row['email']);
		}
		$usedList = implode("','", $escapedNative);

		$otherSql = "SELECT email, active, `421`, `451` FROM `$list` WHERE email NOT IN ('$usedList') ORDER BY rand()";
	} else {
		$otherSql = "SELECT email, active, `421`, `451` FROM `$list` ORDER BY rand()";
	}

	$otherRes = $ext->query($otherSql);
	$others = [];
	while ($row = $otherRes->fetch_assoc()) {
		$others[] = $row;
	}

	echo "Total opener found: " . $openerCount . "\n";
	echo "Front opener: " . count($frontNative) . "\n";
	echo "Back opener: " . count($backNative) . "\n";
	echo "Mid opener (leftover): " . count($midNative) . "\n";
	echo "Mid others (non-opener + leftover opener): " . count($others) . "\n";

	// rebuild output table
	$ext->query("CREATE TABLE IF NOT EXISTS `$sortedList` LIKE `$list`");
	$ext->query("TRUNCATE TABLE `$sortedList`");

	echo "Sorting front opener (" . count($frontNative) . ").\n";
	$frontValues = [];
	foreach ($frontNative as $row) {
		$email  = mysqli_real_escape_string($ext, $row['email']);
		$active = (int)$row['active'];
		$v421   = (int)$row['421'];
		$v451   = (int)$row['451'];
		$frontValues[] = "('$email', $active, $v421, $v451)";
	}

	if (!empty($frontValues)) {
		batchInsert($ext, $sortedList, $frontValues);
	}

	echo "Sorting middle part (" . count($others) . ").\n";
	$midValues = [];
	foreach ($others as $row) {
		$email  = mysqli_real_escape_string($ext, $row['email']);
		$active = (int)$row['active'];
		$v421   = (int)$row['421'];
		$v451   = (int)$row['451'];
		$midValues[] = "('$email', $active, $v421, $v451)";
	}
	if (!empty($midValues)) {
		batchInsert($ext, $sortedList, $midValues);
	}

	echo "Sorting back native (" . count($backNative) . ").\n";
	$backValues = [];
	foreach ($backNative as $row) {
		$email  = mysqli_real_escape_string($ext, $row['email']);
		$active = (int)$row['active'];
		$v421   = (int)$row['421'];
		$v451   = (int)$row['451'];
		$backValues[] = "('$email', $active, $v421, $v451)";
	}
	if (!empty($backValues)) {
		batchInsert($ext, $sortedList, $backValues);
	}

	echo "Done sorting native and fresh.\n";
}

function batchInsert($ext, $table, $values, $chunkSize = 1000) {
	$chunks = array_chunk($values, $chunkSize);
	foreach ($chunks as $chunk) {
		$insQry = "INSERT INTO `$table` (email, active, `421`, `451`) VALUES " . implode(", ", $chunk);
		$ext->query($insQry);
	}
}

function importSeedEmail($ext, $sortedList, $listid) {
	echo "Start import seedcheck email.\n";

	$getSeedSQL = "SELECT seed_email FROM seed_info WHERE seed_id IN (SELECT seed_id FROM seed_allocate WHERE list_id = $listid)";
	$seedRes = $ext->query($getSeedSQL);
	$seedCount = mysqli_num_rows($seedRes);

	if ($seedCount == 0) {
		echo "No seedcheck email found for $sortedList.\n";
		return $sortedList;
	}

	$sql = "SELECT COUNT(*) AS total_email FROM `$sortedList`";
	$tmpRes = $ext->query($sql);
	$tmpRow = $tmpRes->fetch_assoc();
	$emailCount = $tmpRow['total_email'];

	$limitAmount = floor($emailCount / $seedCount);

	$temptable = $sortedList . "_seedtmp";

	$dropTable = "DROP TABLE IF EXISTS `$temptable`";
	$ext->query($dropTable);
	$createTable = "CREATE TABLE `$temptable` LIKE `$sortedList`";
	$ext->query($createTable);

	$start = 0;
	while ($s = $seedRes->fetch_assoc()) {
		$insQry = "INSERT IGNORE INTO `$temptable` (`email`, `active`, `421`, `451`) SELECT `email`, `active`, `421`, `451` FROM `$sortedList` LIMIT $start, $limitAmount";
		$ext->query($insQry);

		$insSeed = "INSERT IGNORE INTO `$temptable` (`email`, `active`, `421`, `451`) VALUES('".$s['seed_email']."', 1, 0, 0)";
		$ext->query($insSeed);

		$start += $limitAmount;
	}

	$balance = $emailCount - $start;

	$insQry = "INSERT IGNORE INTO `$temptable` (`email`, `active`, `421`, `451`) SELECT `email`, `active`, `421`, `451` FROM `$sortedList` LIMIT $start, $balance";
	$ext->query($insQry);

	// afte import seed, drop the sorted table
	$dropSql = "DROP TABLE `$sortedList`";
	$ext->query($dropSql);

	echo "Done import seedcheck email.\n";

	return $temptable;
}

function getOpener($ext, $table, $fetchAll = false) {
	if ($fetchAll) {
		$sql = "SELECT email, active, `421`, `451` FROM `$table` WHERE active = 1 AND email IN (SELECT click_email FROM user_click WHERE click_date >= NOW() - INTERVAL 7 DAY) ORDER BY rand()";
		$res = $ext->query($sql);
		$native = [];
		while ($row = $res->fetch_assoc()) {
			$native[] = $row;
		}
		return $native;
	} else {
		$sql = "SELECT COUNT(*) AS opener_count FROM `$table` WHERE active = 1 AND email IN (SELECT click_email FROM user_click WHERE click_date >= NOW() - INTERVAL 7 DAY)";
		$res = $ext->query($sql);
		$row = $res->fetch_assoc();
		return $row['opener_count'];
	}
}

function swapTable($ext, $targetTable, $finalTable) {
	$ext->query("DROP TABLE `$targetTable`");
	$ext->query("RENAME TABLE `$finalTable` TO `$targetTable`");

	echo "Table swapped successfully.\n";
}

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
	$sql = "SELECT list_id, list_name FROM list WHERE list_group = '$group'";
	$res = $ext->query($sql);

	$arr = [];
	while ($row = $res->fetch_assoc()) {
		$arr[] = $row;
	}

	return $arr;
}

function removeSeed($ext, $list, $listid) {
	$deleteSql = "DELETE FROM `$list` WHERE `email` IN (SELECT `seed_email` FROM `seed_allocate`, `seed_info` WHERE `seed_allocate`.`seed_id` = `seed_info`.`seed_id` AND `list_id` = '".$listid."')";
	$ext->query($deleteSql);
	echo "Seed removed amount: " . mysqli_affected_rows($ext) . "\n";
}

function sortTableIdByGroup($ext, $groups) {
	echo "Start sorting email_id:\n";

	foreach ($groups as $group) {
		echo "Processing group: $group\n";
		$groupSafe = mysqli_real_escape_string($ext, $group);

		$sql = "SELECT list_name  FROM list WHERE list_group = '$groupSafe'";
		$res = $ext->query($sql);

		while ($row = $res->fetch_assoc()) {
			$tableName = $row['list_name'];
			echo "Sorting email_id for: $tableName\n";

			$multiSql  = "SET @count = 0;";
			$multiSql .= "UPDATE `$tableName` SET `email_id` = @count:= @count + 1 ORDER BY `email_id` ASC;";

			if (!$ext->multi_query($multiSql)) {
				echo "Error sorting table $tableName: " . $ext->error . "\n";
				continue;
			}

			// flush multi_query result
			while ($ext->more_results() && $ext->next_result()) {;}
			echo "Done: $tableName\n";
		}
	}
	echo "Done sorting email_id.\n";
}
?>