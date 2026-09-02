<?php
date_default_timezone_set('America/New_York');
require_once __DIR__ . '/../connection2.php';
require_once __DIR__ . '/../callAPILogAlert.php';

checkDuplicateCron();

$qryJob = "SELECT job_id, group_name, next_rotation_date, next_section
	FROM seed_rotation_job
	WHERE is_active = '1' AND next_rotation_date = current_date";
$jobRes = $conn->query($qryJob);

if ($jobRes->num_rows === 0) {
	die("No job record. Nothing to process.\n");
}

$jobs = [];
while($row = $jobRes->fetch_assoc()) {
	$jobs[] = $row;
}

$maxChildren = 4;
$children = [];

foreach ($jobs as $job) {
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
		processJob($job);
		exit(0); // must exit, never let child fall through to parent loop
	} else {
		// parent process
		$children[$pid] = true;
		echo "[".date('H:i:s')."] Parent: child $pid created for {$job['group_name']}.\n";
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

echo "All jobs completed.\n";

function connectPigeonMail($pgMail) {
	$host = "$pgMail.net";
	$username = $pgMail;
	$password = "geekgeek50509";
	$db = "pigeon_mail";

	$pgConn = mysqli_init();

	// Set connection timeout to 10 seconds
	$pgConn->options(MYSQLI_OPT_CONNECT_TIMEOUT, 10);

	// Optional: faster DNS timeout behavior
	$pgConn->real_connect($host, $username, $password, $db);

	if ($pgConn->connect_error) {
		throw new Exception("Failed to connect to $pgMail: " . $pgConn->connect_error . "\n");
	}

	return $pgConn;
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

function insertJobLog($local, $jobId, $executionDate, $section, $startDateTime) {
	$jobIdEsc = $local->real_escape_string($jobId);
	$execDateEsc = $local->real_escape_string($executionDate);
	$sectionEsc = $local->real_escape_string($section);
	$startEsc = $local->real_escape_string($startDateTime);
	$status = 'RUNNING';

	$sql = "INSERT INTO seed_rotation_run (job_id, execution_date, section, status, started_datetime)
			VALUES ('$jobIdEsc', '$execDateEsc', '$sectionEsc', '$status', '$startEsc')";
	$local->query($sql);
	return $local->insert_id;
}

function updateJobLog($local, $runId, $status, $message = null) {
	$statusEsc = $local->real_escape_string($status);
	$messageEsc = isset($message) ? $local->real_escape_string($message) : '';
	$endEsc = date('Y-m-d H:i:s');
	$runIdEsc = (int)$runId;

	$sql = "UPDATE seed_rotation_run
			SET status = '$statusEsc', remark = '$messageEsc', ended_datetime = '$endEsc'
			WHERE run_id = $runIdEsc";
	$local->query($sql);
}

function getSendingVolume($ext, $date) {
	$dateEsc = $ext->real_escape_string($date);

	$sql = "SELECT m.message_schedule_date, SUM(mj.job_amount) AS total_amt, SUM(mj.job_ok_sent) AS total_ok_sent
			FROM message m
			INNER JOIN message_job mj ON mj.message_id = m.message_id
			WHERE m.message_schedule_date = '$dateEsc'
			AND m.message_group LIKE '%real%'
			AND m.message_status = 'completed'
			GROUP BY m.message_schedule_date";
	$res = $ext->query($sql);
	$row = $res->fetch_assoc();

	if ($row === null || $row['total_amt'] === null) {
		return null;
		// skip job if no sending volume found
	}
	return $row;
}

function getInboxRate($ext, $date) {
	$dateEsc = $ext->real_escape_string($date);

	$sql = "SELECT SUM(CASE WHEN sd.deliver_result = 'inbox' THEN 1 ELSE 0 END) AS inbox_count,
				SUM(CASE WHEN sd.deliver_result = 'junk' THEN 1 ELSE 0 END) AS junk_count,
				ROUND(
					SUM(CASE WHEN sd.deliver_result = 'inbox' THEN 1 ELSE 0 END) /
					NULLIF(SUM(CASE WHEN sd.deliver_result IN ('inbox', 'junk') THEN 1 ELSE 0 END), 0) * 100, 2
				) AS inbox_rate
			FROM seed_delivery2 sd
			INNER JOIN domain d ON d.domain_id = sd.domain_id
			WHERE sd.deliver_date = '$dateEsc'
			AND sd.list_group LIKE '%real%'";

	$res = $ext->query($sql);
	$row = $res->fetch_assoc();

	if ($row === null || $row['inbox_rate'] === null) {
		return null;
		// skip job if no inbox rate found
	}

	$row['inbox_rate']   = (float)$row['inbox_rate'];
	$row['notspam_rate'] = round(100 - $row['inbox_rate'], 2);

	return $row;
}

function calculateMinimumSeed($sendingVolume, $notspamRate) {
	$notspamSeed = round($sendingVolume * ($notspamRate / 100));
	$minSeed = (int) ceil($notspamSeed * 0.02); // 2% not spam rate

	return $minSeed;
}

function getMessageGroupsBySection($ext, $date, $section) {
	$dateEsc = $ext->real_escape_string($date);

	$sql = "SELECT DISTINCT m.message_group, mj.job_schedule_date
			FROM message m
			INNER JOIN message_job mj ON m.message_id = mj.message_id
			WHERE m.message_schedule_date = '$dateEsc'
			AND m.message_status = 'scheduled'
			AND m.message_group LIKE '%train%'
			ORDER BY mj.job_schedule_date";
	$res = $ext->query($sql);

	$rows = [];
	while ($row = $res->fetch_assoc()) {
		$rows[] = $row;
	}

	if (empty($rows)) {
		return ['status' => 'no_data', 'message_groups' => [], 'schedule_date' => null];
	}

	// get distinct schedule times
	$times = array_values(array_unique(array_column($rows, 'job_schedule_date')));
	sort($times);

	// only one distinct time = only head or tail is set
	// if (count($times) === 1) {
	// 	$groups = array_values(array_unique(array_column($rows, 'message_group')));
	// 	return [
	// 		'status' => 'only_one_section',
	// 		'message_groups' => $groups,
	// 		'schedule_date' => $times[0],
	// 	];
	// }

	// more than 2 distinct time
	// if (count($times) > 2) {
	// 	echo count($times) . " distinct scheduled times, expected 2 (head/tail) only.\n";
	// }

	$headTime = $times[0];
	$tailTime = $times[count($times) - 1];
	$targetTime = ($section === 'head') ? $headTime : $tailTime;

	$groups = [];
	foreach ($rows as $row) {
		if ($row['job_schedule_date'] === $targetTime) {
			$groups[] = $row['message_group'];
		}
	}
	$groups = array_values(array_unique($groups));

	return [
		'message_groups' => $groups,
		'schedule_date' => $targetTime,
	];
} 

function getTotalSeedByMessageGroups($ext, array $messageGroups) {
	$escGroups = [];
	foreach ($messageGroups as $group) {
		$escGroups[] = "'" . $ext->real_escape_string($group) . "'";
	}

	$inGroup = implode(", ", $escGroups);
	$listNames = [];

	$sql = "SELECT list_name FROM list WHERE list_group IN ($inGroup)";
	$res = $ext->query($sql);

	while ($row = $res->fetch_assoc()) {
		$listNames[] = $row['list_name'];
	}

	// get total seed from each list
	$totalSeed = 0;
	$details = [];
	foreach ($listNames as $listName) {
		$sql = "SELECT COUNT(1) AS total_seed FROM `$listName` WHERE active = 1";
		$res = $ext->query($sql);

		$row = $res->fetch_assoc();
		$seedCount = (int) $row['total_seed'];

		$details[$listName] = $seedCount;
		$totalSeed += $seedCount;
	}

	return [
		'total_seed' => $totalSeed,
		'list_names' => $listNames,
		'details' => $details,
	];
}

function buildAndSaveRotationPlan($local, array $extConnections, $jobId, $section, $runId, array $messageGroupsByServer, array $serverDomain) {
	// $domains = getRotationDomainsAndLists($ext, $messageGroups);

	/*if (count($domains) < 2) {
		return [
			'planned' => 0,
			'message' => 'At least two domains are required.'
		];
	}*/

	$totalPlanned = 0;
	$domainCount = count($serverDomain);

	$domainLists = [];
	foreach ($extConnections as $pigeonMail => $ext) {
		$domainsOnServer = getRotationDomainsAndLists($ext, $messageGroupsByServer[$pigeonMail]);
		foreach ($domainsOnServer as $domainInfo) {
			$domainLists[$pigeonMail][$domainInfo['domain_name']] = $domainInfo;
		}
	}

	$eligibleSeedsByDomain = [];
	$totalCounts = [];
	$eligibleCounts = [];

	// save each domain's original seeds before rotation
	for ($i = 0; $i < $domainCount; $i++) {
		$sourceDomain = $serverDomain[$i];
		$sourceServer = $sourceDomain['pigeon_mail'];
		$sourceExt = $extConnections[$sourceServer];
		$sourceDomainInfo = $domainLists[$sourceServer][$sourceDomain['domain_name']];
		$domainKey = $sourceServer . '|' . $sourceDomain['domain_name'];

		$destinationIndex = ($i + 1) % $domainCount;
		$destinationDomain = $serverDomain[$destinationIndex];
		$destinationServer = $destinationDomain['pigeon_mail'];

		echo "\nRotation: " . $sourceDomain['domain_name'] . " to " . $destinationDomain['domain_name'] . "\n";

		// get seed email from the list
		$sourceSeeds = getSeedsFromDomainLists($sourceExt, $sourceDomainInfo);

		echo "Source seeds found: " . count($sourceSeeds) . "\n";

		$eligibleSeeds = []; // store seed that's eligble to rotate
		foreach ($sourceSeeds as $seed) {
			if (canSeedRotateToDomain($local, $jobId, $section, $seed['seed_email'], $destinationServer, $destinationDomain['domain_name'])) {
				$eligibleSeeds[] = $seed;
			} 
			// else {
			// 	echo "Skip seed " . $seed['seed_email'] . ": destination domain " . $destinationDomain['domain_name'] . " not eligible to rotate.\n";
			// }
		}
		// print_r($eligibleSeeds);
		echo "Eligible seeds for rotation: " . count($eligibleSeeds) . "\n";

		$eligibleSeedsByDomain[$domainKey] = $eligibleSeeds;
		$totalCounts[$domainKey] = count($sourceSeeds);
		$eligibleCounts[$domainKey] = count($eligibleSeeds);
	}

	// decide which domains are actually allowed to rotate
	$sendable = checkDomainRotate($serverDomain, $totalCounts, $eligibleCounts);

	for ($i = 0; $i < $domainCount; $i++) {
		$sourceDomain = $serverDomain[$i];
		$sourceServer = $sourceDomain['pigeon_mail'];
		$domainKey = $sourceServer . '|' . $sourceDomain['domain_name'];

		$destinationIndex = ($i + 1) % $domainCount;
		$destinationDomain = $serverDomain[$destinationIndex];
		$destinationServer = $destinationDomain['pigeon_mail'];
		$destinationDomainInfo = $domainLists[$destinationServer][$destinationDomain['domain_name']];

		echo "\nRotation: " . $sourceDomain['domain_name'] . " to " . $destinationDomain['domain_name'] . "\n";

		if ($eligibleCounts[$domainKey] === 0) {
			echo "Skip: no eligible seed to rotate for " . $sourceDomain['domain_name'] . ".\n";
			continue;
		}

		if (!$sendable[$domainKey]) {
			echo "Warning: " . $sourceDomain['domain_name'] . " would end up with zero seeds this round "
				. "(all " . $totalCounts[$domainKey] . " seed(s) eligible, but predecessor domain cannot rotate seed out). "
				. "Seeds stay in current domain.\n";
			continue;
		}

		$eligibleSeeds = $eligibleSeedsByDomain[$domainKey];
		echo "Source seeds found: " . $totalCounts[$domainKey] . "\n";
		echo "Eligible seeds for rotation: " . count($eligibleSeeds) . "\n";

		// get the destination for the source seed email
		$destinationLists = $destinationDomainInfo['lists'];

		echo "Destination lists: " . count($destinationLists) . "\n";

		// distribute seed equally to destination list
		$distribution = calculateListDistribution(count($eligibleSeeds), count($destinationLists));

		$offset = 0;
		foreach ($destinationLists as $index => $destinationList) {
			$limit = $distribution[$index];

			echo "Allocate $limit seed(s) to " . $destinationList['list_name'] . "\n";

			if ($limit <= 0) {
				continue;
			}

			$allocatedSeeds = array_slice($eligibleSeeds, $offset, $limit);

			foreach ($allocatedSeeds as $seed) {
				insertPlannedSeedMovement($local, $runId, $sourceServer, $destinationServer, $seed, $destinationDomain, $destinationList);
				$totalPlanned++;
			}

			$offset += $limit;
		}
	}

	echo "\nTotal planned movements: $totalPlanned\n";

	return [
		'planned' => $totalPlanned,
		'message' => "$totalPlanned seed movements planned."
	];
}

function getRotationDomainsAndLists($ext, array $messageGroups) {
	$escapedGroups = [];
	foreach ($messageGroups as $group) {
		$escapedGroups[] = "'" . $ext->real_escape_string($group) . "'";
	}

	$groupSql = implode(',', $escapedGroups);
	$sql = "SELECT d.domain_id, d.domain_domain AS domain_name, l.list_id, l.list_name, l.list_group
			FROM list l
			INNER JOIN domain d ON d.domain_id = l.domain_id
			WHERE l.list_group IN ($groupSql)
			ORDER BY d.domain_id";
	$res = $ext->query($sql);

	$domains = [];
	while ($row = $res->fetch_assoc()) {
		$domainId = (int) $row['domain_id'];

		if (!isset($domains[$domainId])) {
			$domains[$domainId] = [
				'domain_id' => $domainId,
				'domain_name' => $row['domain_name'],
				'lists' => [],
			];
		}

		$domains[$domainId]['lists'][] = [
			'list_id' => (int) $row['list_id'],
			'list_name' => $row['list_name'],
			'list_group' => $row['list_group'],
		];
	}

	return array_values($domains);
}

function getSeedsFromDomainLists($ext, array $domain) {
	$seeds = [];

	foreach ($domain['lists'] as $list) {
		$tableName = $list['list_name'];

		$sql = "SELECT l.email AS seed_email, si.seed_id
				FROM `$tableName` l
				LEFT JOIN seed_info si ON l.email = si.seed_email
				WHERE l.active = 1
				ORDER BY l.email_id";
		$res = $ext->query($sql);

		while ($row = $res->fetch_assoc()) {
			$seeds[] = [
				'seed_id' => $row['seed_id'] !== null ? (int) $row['seed_id'] : null,
				'seed_email' => $row['seed_email'],
				'source_list_id' => $list['list_id'],
				'source_list_name' => $list['list_name'],
				'source_domain_name' => $domain['domain_name'],
			];
		}
	}

	return $seeds;
}

function calculateListDistribution($totalSeeds, $totalLists) {
	$base = (int)($totalSeeds / $totalLists);
	$remainder = $totalSeeds % $totalLists;

	$distribution = [];

	for ($i = 0; $i < $totalLists; $i++) {
		$distribution[$i] = $base;

		if ($i < $remainder) {
			$distribution[$i]++;
		}
	}

	return $distribution;
}

function insertPlannedSeedMovement($local, $runId, $fromServer, $toServer, array $seed, array $destinationDomain, array $destinationList) {
	$runId = (int) $runId;

	$seedId = $seed['seed_id'] !== null ? (int) $seed['seed_id'] : 'NULL';
	$fromListId = (int) $seed['source_list_id'];
	$toListId = (int) $destinationList['list_id'];

	$fromServerEsc = $local->real_escape_string($fromServer);
	$toServerEsc = $local->real_escape_string($toServer);
	$seedEmailEsc = $local->real_escape_string($seed['seed_email']);
	$sourceDomainNameEsc = $local->real_escape_string($seed['source_domain_name']);
	$sourceListEsc = $local->real_escape_string($seed['source_list_name']);
	$destinationDomainNameEsc = $local->real_escape_string($destinationDomain['domain_name']);
	$destinationListEsc = $local->real_escape_string($destinationList['list_name']);

	$plannedDatetime = date('Y-m-d H:i:s');

	$sql = "INSERT INTO seed_rotation_log (
				run_id,
				seed_email,
				seed_id,
				from_server,
				from_domain,
				from_list,
				from_list_id,
				to_server,
				to_domain,
				to_list,
				to_list_id,
				movement_status,
				planned_datetime
			) VALUES (
				$runId,
				'$seedEmailEsc',
				$seedId,
				'$fromServerEsc',
				'$sourceDomainNameEsc',
				'$sourceListEsc',
				$fromListId,
				'$toServerEsc',
				'$destinationDomainNameEsc',
				'$destinationListEsc',
				$toListId,
				'PLANNED',
				'$plannedDatetime'
			)";

	if (!$local->query($sql)) {
		throw new Exception("Failed to save seed rotation plan: " . $local->error);
	}

	return $local->insert_id;
}

function executePlannedRotation($local, array $extConnections, $runId) {
	$runId = (int) $runId;

	$sql = "SELECT log_id, seed_id, seed_email, from_server, from_list_id, from_list, to_server, to_list_id, to_list
			FROM seed_rotation_log
			WHERE run_id = $runId
			AND movement_status = 'PLANNED'
			ORDER BY log_id";
	$res = $local->query($sql);

	$result = [
		'completed' => 0,
		'insert_failed' => 0,
		'delete_failed' => 0,
		'seed_not_found' => 0
	];

	while ($movement = $res->fetch_assoc()) {
		if ($movement['seed_id'] === null) {
			logAllocateError($local, $movement['log_id'], 'Seed not found in seed_info');
			echo "Skip for {$movement['seed_email']}: Seed not found in seed_info\n";
			$result['seed_not_found']++;
			continue;
		}

		$sourceExt = $extConnections[$movement['from_server']];
		$destinationExt = $extConnections[$movement['to_server']];

		// insert to destination (to_list)
		$insertResult = insertPlannedSeedToDestination($destinationExt, $movement);

		if (!$insertResult['success']) {
			updateMovementStatus($local, $movement['log_id'], 'INSERT_FAILED', $insertResult['error']);
			$result['insert_failed']++;
			continue;
		}

		// delete from source (from_list)
		$deleteResult = deletePlannedSeedFromSource($sourceExt, $movement);

		if (!$deleteResult['success']) {
			updateMovementStatus($local, $movement['log_id'], 'DELETE_FAILED', $deleteResult['error']);
			$result['delete_failed']++;
			continue;
		}

		// update rotation log
		completeMovement($local, $movement['log_id']);

		$isCrossServer = ($movement['from_server'] !== $movement['to_server']);
		$destinationSeedId = $movement['seed_id']; // same-server use the existing seed_id

		if ($isCrossServer) {
			// echo "rotate cross server: {$movement['seed_id']}\n";
			$sourceSeedInfo = getSeedInfoById($sourceExt, $movement['seed_id']);

			if ($sourceSeedInfo === null) {
				logAllocateError($local, $movement['log_id'], 'Seed not found in source seed_info during cross-server move');
			} else {
				$seedInfoInsertResult = insertSeedInfoToDestination($destinationExt, $sourceSeedInfo, $movement['to_server']);

				if (!$seedInfoInsertResult['success']) {
					logAllocateError($local, $movement['log_id'], 'Failed to insert seed_info on destination: ' . $seedInfoInsertResult['error']);
				} else {
					$seedInfoDeleteResult = deleteSeedInfoFromSource($sourceExt, $movement['seed_id']);

					if (!$seedInfoDeleteResult['success']) {
						logAllocateError($local, $movement['log_id'], 'Failed to delete seed_info on source: ' . $seedInfoDeleteResult['error']);
					}

					// use the destination's seed_id
					$destinationSeedId = (int) $seedInfoInsertResult['seed_id'];

					// handle seed proxy
					$sourceSeedProxy = getSeedProxyById($sourceExt, $movement['seed_id']);

					if ($sourceSeedProxy !== null) {
						$seedProxyInsertResult = insertSeedProxyToDestination($destinationExt, $sourceSeedProxy, $destinationSeedId);

						if (!$seedProxyInsertResult['success']) {
							logAllocateError($local, $movement['log_id'], 'Failed to insert seed_proxy on destination: ' . $seedProxyInsertResult['error']);
						} else {
							$seedProxyDeleteResult = deleteSeedProxyFromSource($sourceExt, $movement['seed_id']);

							if (!$seedProxyDeleteResult['success']) {
								logAllocateError($local, $movement['log_id'], 'Failed to delete seed_proxy on source: ' . $seedProxyDeleteResult['error']);
							}
						}
					}
				}
			}
		}

		// udpate seed allocate
		$allocateResult = updateSeedAllocate($sourceExt, $destinationExt, (int) $movement['seed_id'], $destinationSeedId, (int) $movement['from_list_id'], (int) $movement['to_list_id']);

		if (!$allocateResult['success']) {
			logAllocateError($local, $movement['log_id'], $allocateResult['error']);
		}

		$result['completed']++;
	}

	return $result;
}

function canSeedRotateToDomain($local, $jobId, $section, $seedEmail, $server, $destinationDomain) {
	$jobId = (int) $jobId;
	$sectionEsc = $local->real_escape_string($section);
	$seedEmailEsc = $local->real_escape_string($seedEmail);
	$serverEsc = $local->real_escape_string($server);
	$destinationDomainEsc = $local->real_escape_string($destinationDomain);

	$sqlPending = "SELECT l.log_id
			FROM seed_rotation_log l
			INNER JOIN seed_rotation_run r ON r.run_id = l.run_id
			WHERE r.job_id = $jobId
			AND r.section = '$sectionEsc'
			AND l.seed_email = '$seedEmailEsc'
			AND l.movement_status IN ('PLANNED', 'DELETE_FAILED')
			LIMIT 1";
	$pendingRes = $local->query($sqlPending);

	if ($pendingRes->num_rows > 0) {
		return false;
	}

	$sqlVisited = "SELECT l.log_id
			FROM seed_rotation_log l
			INNER JOIN seed_rotation_run r ON r.run_id = l.run_id
			WHERE r.job_id = $jobId
			AND r.section = '$sectionEsc'
			AND l.seed_email = '$seedEmailEsc'
			AND l.movement_status = 'COMPLETED'
			AND (
				(l.from_server = '$serverEsc' AND l.from_domain = '$destinationDomainEsc')
				OR
				(l.to_server = '$serverEsc' AND l.to_domain = '$destinationDomainEsc')
			)
			AND l.completed_datetime > DATE_SUB(NOW(), INTERVAL 70 DAY)
			LIMIT 1";
	$visitedRes = $local->query($sqlVisited);

	return $visitedRes->num_rows === 0;
}

function completeMovement($local, $logId) {
	$logId = (int) $logId;
	$endTime = date('Y-m-d H:i:s');

	$sql = "UPDATE seed_rotation_log
			SET movement_status = 'COMPLETED',
				completed_datetime = '$endTime',
				error_message = NULL
			WHERE log_id = $logId
			AND movement_status = 'PLANNED'";

	return $local->query($sql);
}

function updateMovementStatus($local, $logId, $status, $errorMessage) {
	$logId = (int) $logId;

	$statusEsc = $local->real_escape_string($status);
	$errorEsc = $local->real_escape_string($errorMessage);

	$sql = "UPDATE seed_rotation_log
			SET movement_status = '$statusEsc',
			error_message = '$errorEsc'
			WHERE log_id = $logId";

	return $local->query($sql);
}

function insertPlannedSeedToDestination($ext, $movement) {
	$toTable = $movement['to_list'];
	$emailEsc = $ext->real_escape_string($movement['seed_email']);

	$sql = "INSERT INTO `$toTable` (email, active, `421`, `451`)
			VALUES ('$emailEsc', 1, 0, 0)";

	if (!$ext->query($sql)) {
		return [
			'success' => false,
			'error' => $ext->error
		];
	}

	return [
		'success' => true
	];
}

function deletePlannedSeedFromSource($ext, $movement) {
	$fromTable = $movement['from_list'];
	$emailEsc = $ext->real_escape_string($movement['seed_email']);

	$sql = "DELETE FROM `$fromTable` WHERE email = '$emailEsc' LIMIT 1";

	if (!$ext->query($sql)) {
		return [
			'success' => false,
			'error' => $ext->error
		];
	}

	if ($ext->affected_rows != 1) {
		return [
			'success' => false,
			'error' => "Deleted ".$ext->affected_rows." row(s)"
		];
	}

	return [
		'success' => true
	];
}

function updateNextRotationSchedule($local, $jobId, $currentSection) {
	$nextSection = ($currentSection === 'head') ? 'tail' : 'head';

	$jobIdInt = (int) $jobId;
	$nextSectionEsc = $local->real_escape_string($nextSection);

	// run -> stop 3 day -> run
	$sql = "UPDATE seed_rotation_job
			SET next_section = '$nextSectionEsc', 
			next_rotation_date = DATE_ADD(next_rotation_date, INTERVAL 4 DAY)
			WHERE job_id = $jobIdInt";

	if (!$local->query($sql)) {
		throw new Exception("Failed to update next rotation schedule for job $jobIdInt: " . $local->error);
	}

	return ['next_section' => $nextSection];
}

function updateSeedAllocate($sourceExt, $destinationExt, $sourceSeedId, $destinationSeedId, $fromListId, $toListId) {
	// check existing seed_allocate, do delete and insert (in case seed_id is already found with list_id)
	$sqlExisting = "SELECT check_type FROM seed_allocate WHERE seed_id = $sourceSeedId AND list_id = $fromListId LIMIT 1";
	$resExisting = $sourceExt->query($sqlExisting);

	if ($resExisting && $resExisting->num_rows > 0) {
		$checkType = $resExisting->fetch_assoc()['check_type'];

		$sqlDelete = "DELETE FROM seed_allocate WHERE seed_id = $sourceSeedId AND list_id = $fromListId";
		if (!$sourceExt->query($sqlDelete)) {
			return ['success' => false, 'error' => "Failed to delete seed_allocate row for seed_id=$sourceSeedId, list_id=$fromListId: " . $sourceExt->error];
		}
	} else {
		$checkType = 'both';
	}

	$sqlUpsert = "INSERT INTO seed_allocate (seed_id, list_id, check_type)
			VALUES ($destinationSeedId, $toListId, '$checkType')
			ON DUPLICATE KEY UPDATE check_type = VALUES(check_type)";

	if (!$destinationExt->query($sqlUpsert)) {
		return ['success' => false, 'error' => "Failed to upsert seed_allocate row for seed_id=$destinationSeedId, list_id=$toListId: " . $destinationExt->error];
	}

	return ['success' => true, 'error' => null];
}

function logAllocateError($local, $logId, $message) {
	$logId = (int) $logId;
	$messageEsc = $local->real_escape_string($message);

	$sql = "UPDATE seed_rotation_log
			SET error_message = '$messageEsc'
			WHERE log_id = $logId";

	return $local->query($sql);
}

function sortTableIdByGroup($ext, array $groups) {
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

function checkDuplicateCron() {
	$currentPid = getmypid();
	$parentPid = function_exists('posix_getppid') ? posix_getppid() : null;

	exec("pgrep -f seed_rotation_job.php", $pids);
	$pids = array_filter(array_map('trim', $pids), function($p) { return $p !== ''; });

	$otherPids = array_filter($pids, function($pid) use ($currentPid, $parentPid) {
		$pid = (int) $pid;
		if ($pid === $currentPid) return false;
		if ($parentPid !== null && $pid === $parentPid) return false;
		return true;
	});

	if (count($otherPids) > 0) {
		$emailMsg = "Found old seed rotation script running, new job will not trigger.";
		$data = array(
			"server_name" => "centralscheduler",
			"module_name" => "Seed Rotation Job",
			"alert_message" => $emailMsg,
			"alert_code" => "1001",
			"severity"=> "Critical"
		);

		$response = call_AlertAPI($data);
		if ($response['status'] == "fail"){
			echo "Error when calling API endpoint: " . $response['data'];
		}
		else {
			echo "Success";
		}
		exit;
	}
}

function processJob(array $job) {
	require __DIR__ . '/../connection2.php';

	$jobId = $job['job_id'];
	$groupJob = $job['group_name'];
	$section = $job['next_section'];

	$startTimestamp = time();
	$startDateTime = date('Y-m-d H:i:s', $startTimestamp);
	$executionDate = date('Y-m-d', $startTimestamp);
	// $prevDate = date('Y-m-d', strtotime('-1 day', $startTimestamp));
	$today = date('Y-m-d');

	$logData = [];

	echo "Start: $startDateTime\n";
	echo "Processing on group job: $groupJob\n";

	$runId = insertJobLog($conn, $jobId, $executionDate, $section, $startDateTime);

	// get the servers of the group
	$serverList = getGroupServers($conn, $jobId);

	$extConnections = [];
	foreach ($serverList as $pigeonMail) {
		try {
			$extConnections[$pigeonMail] = connectPigeonMail($pigeonMail);
		} catch (Exception $e) {
			echo "Skipping group $groupJob: " . $e->getMessage();
			updateJobLog($conn, $runId, 'ERROR', $e->getMessage());
			foreach ($extConnections as $openConn) {
				$openConn->close();
			}
			$conn->close();
			return;
		}
	}

	// check if any sending in progress
	foreach ($extConnections as $pigeonMail => $extConn) {
		if (checkSending($extConn)) {
			echo "Sending in progress on $pigeonMail.\n";
			updateJobLog($conn, $runId, 'SKIPPED', 'Sending in progress on $pigeonMail');
			foreach ($extConnections as $openConn) {
				$openConn->close();
			}
			$conn->close();
			return;
		}
	}

	/* not required for now
	$volumeData = getSendingVolume($extConn, $prevDate);
	$inboxData = getInboxRate($extConn, $prevDate);

	if ($volumeData === null || $inboxData === null) {
		$missing = [];
		if ($volumeData === null) $missing[] = 'sending volume';
		if ($inboxData === null)  $missing[] = 'inbox rate';
		$msg = "No " . implode(' and ', $missing) . " data found for $prevDate";

		echo "$msg\n";
		updateJobLog($conn, $runId, 'SKIPPED', $msg);
		$extConn->close();
		return;
	}

	$sendingVol = (float) $volumeData['total_ok_sent'];
	$notspamRate = (float) $inboxData['notspam_rate'];
	$minimumSeed = calculateMinimumSeed($sendingVol, $notspamRate);

	$logData['sending_volume'][] = $sendingVol;
	$logData['inbox_rate'][] = $inboxData['inbox_rate'];
	$logData['mininum_seed'][] = $minimumSeed;

	echo "Min notspam seed required for rotation: $minimumSeed\n"; */

	// get scheduled train message group by head/tail for each server
	$messageGroupsByServer = [];
	foreach ($extConnections as $pigeonMail => $extConn) {
		$result = getMessageGroupsBySection($extConn, $today, $section);

		if (count($result['message_groups']) == 0) {
			$msg = "No train group message is scheduled on {$today} for server $pigeonMail.";
			echo "$msg\n";
			updateJobLog($conn, $runId, 'SKIPPED', $msg);
			foreach ($extConnections as $openConn) {
				$openConn->close();
			}
			$conn->close();
			return;
		}

		$messageGroupsByServer[$pigeonMail] = $result['message_groups'];
	}

	// check any new domain or domain replacement
	foreach ($extConnections as $pigeonMail => $extConn) {
		$syncResult = syncDomainSequence($conn, $extConn, $jobId, $pigeonMail, $messageGroupsByServer[$pigeonMail]);

		echo $syncResult['message'] . "\n";

		if (!$syncResult['success']) {
			updateJobLog($conn, $runId, 'SKIPPED', $syncResult['message']);
			foreach ($extConnections as $openConn) {
				$openConn->close();
			}
			$conn->close();
			return;
		}
	}

	// get all domain from one or multiple server
	$serverDomain = getGroupDomain($conn, $jobId, $serverList);

	// get total seed by head/tail
	// $seedResult = getTotalSeedByMessageGroups($extConn, $result['message_groups']);

	/* $warning = "";
	if ($seedResult['total_seed'] < $minimumSeed) {
		$needToAdd = $minimumSeed - $seedResult['total_seed'];
		// save warning msg in log table
		$warning = "Currently total notspam seed is " . $seedResult['total_seed'] . ". Minimum notspam seed required for rotation is " . $minimumSeed . ". Seed to add: " . $needToAdd;
	} */

	// print_r($logData);

	// build the plan insert to log table with 'PLANNED'
	$planResult = buildAndSaveRotationPlan($conn, $extConnections, $jobId, $section, $runId, $messageGroupsByServer, $serverDomain);

	// planned
	// $logData['total_seed_rotated'][] = $planResult['planned'];

	// start seed rotation
	$rotationResult = executePlannedRotation($conn, $extConnections, $runId);

	$summary = "Planned: {$planResult['planned']}. "
		. "Completed: {$rotationResult['completed']}. "
		. "Insert failed: {$rotationResult['insert_failed']}. "
		. "Delete failed: {$rotationResult['delete_failed']}.";

	/* if ($warning !== "") {
		$summary .= " WARNING: $warning";
	} */

	echo $summary . "\n";

	updateJobLog($conn, $runId, 'COMPLETED', $summary);

	$nextSchedule = updateNextRotationSchedule($conn, $jobId, $section);
	echo "Next rotation section: ({$nextSchedule['next_section']})\n";

	// sort email id
	foreach ($extConnections as $pigeonMail => $extConn) {
		sortTableIdByGroup($extConn, $messageGroupsByServer[$pigeonMail]);
	}

	foreach ($extConnections as $extConn) {
		$extConn->close();
	}
	$conn->close();
}

function checkDomainRotate(array $domains, array $totalCounts, array $eligibleCounts) {
	$domainCount = count($domains);
	$domainIds = array_map(function($d) { return $d['pigeon_mail'] . '|' . $d['domain_name']; }, $domains);

	$send = [];
	$fullyEligible = [];

	foreach ($domainIds as $idx => $domainId) {
		$total = isset($totalCounts[$domainId]) ? $totalCounts[$domainId] : 0;
		$eligible = isset($eligibleCounts[$domainId]) ? $eligibleCounts[$domainId] : 0;


		$send[$domainId] = ($eligible > 0); // eligible rotation seed for destination domain based on log (allow partial, eligible could be lesser than total)
		$fullyEligible[$domainId] = ($total > 0 && $eligible === $total); // source seed vs destination seed
	}

	// keep scanning until nothing changes
	for ($pass = 0; $pass < $domainCount; $pass++) {
		$changed = false;

		for ($i = 0; $i < $domainCount; $i++) {
			$domainId = $domainIds[$i];
			$predecessorId = $domainIds[($i - 1 + $domainCount) % $domainCount];

			// to check current domain able to rotate seed out, predecessor domain also have to be able to rotate seed out to current domain
			if ($fullyEligible[$domainId] && $send[$domainId] && !$send[$predecessorId]) {
				$send[$domainId] = false;
				$changed = true;
			}
		}

		if (!$changed) {
			break;
		}
	}

	return $send;
}

function getGroupServers($local, $jobId) {
	$jobId = (int) $jobId;

	$sql = "SELECT pigeon_mail
			FROM seed_rotation_job_server
			WHERE job_id = $jobId AND is_active = '1'
			ORDER BY id ASC";
	$res = $local->query($sql);

	$servers = [];
	while ($row = $res->fetch_assoc()) {
		$servers[] = $row['pigeon_mail'];
	}

	return $servers;
}

function syncDomainSequence($local, $ext, $jobId, $pigeonMail, array $messageGroups) {
	$jobId = (int) $jobId;
	$pigeonMailEsc = $local->real_escape_string($pigeonMail);

	// live domains currently set to sending on this server for these message groups
	$liveDomains = getRotationDomainsAndLists($ext, $messageGroups);
	$liveDomainNames = array_map(function($d) { return $d['domain_name']; }, $liveDomains);

	// get sending domain recorded in sequence table for this server
	$sql = "SELECT domain_name, domain_position
			FROM seed_rotation_domain_sequence
			WHERE job_id = $jobId AND pigeon_mail = '$pigeonMailEsc' AND is_active = '1'
			ORDER BY domain_position ASC";
	$res = $local->query($sql);

	$recordedDomains = [];
	while ($row = $res->fetch_assoc()) {
		$recordedDomains[] = $row['domain_name'];
	}

	$missing = array_values(array_diff($recordedDomains, $liveDomainNames));
	$new = array_values(array_diff($liveDomainNames, $recordedDomains));

	// first time: record all live domains to seed_rotation_domain_sequence
	if (empty($recordedDomains)) {
		$position = 1;
		foreach ($liveDomainNames as $domainName) {
			insertDomainSequence($local, $jobId, $pigeonMail, $domainName, $position);
			$position++;
		}

		return ['success' => true, 'message' => "Inserted " . count($liveDomainNames) . " domain(s) for $pigeonMail."];
	}

	// in case domain count is mismatch
	/* if (count($missing) !== count($new)) {
		return [
			'success' => false,
			'message' => "Domain count mismatch on server $pigeonMail: " . count($missing) . " missing, " . count($new) . " new."
		];
	} */

	// no domain changes this round
	if (empty($missing) && empty($new)) {
		return ['success' => true, 'message' => "No domain change detected for $pigeonMail."];
	}

	// 3 scenario of domain: missing = new, missing > new, missing < new (new domain inherits missing domain's position)
	$totalDomain = min(count($missing), count($new));

	for ($i = 0; $i < $totalDomain; $i++) {
		$missingDomain = $missing[$i];
		$newDomain = $new[$i];

		$position = getDomainSequencePosition($local, $jobId, $pigeonMail, $missingDomain);

		// after replace the dead domain, set to is_active=0
		retireDomainSequence($local, $jobId, $pigeonMail, $missingDomain);

		// insert the new replacement domain
		insertDomainSequence($local, $jobId, $pigeonMail, $newDomain, $position);
	}

	// new > missing: leftover new domain(s) get appended to the end of this server's own block
	if (count($new) > $totalDomain) {
		$nextPosition = getMaxDomainPosition($local, $jobId, $pigeonMail) + 1;

		for ($i = $totalDomain; $i < count($new); $i++) {
			echo $nextPosition . " | " . $new[$i] . "\n";
			insertDomainSequence($local, $jobId, $pigeonMail, $new[$i], $nextPosition);
			$nextPosition++;
		}
	}

	// missing > new: leftover missing domain(s) just retire, no replacement inserted
	if (count($missing) > $totalDomain) {
		for ($i = $totalDomain; $i < count($missing); $i++) {
			retireDomainSequence($local, $jobId, $pigeonMail, $missing[$i]);
		}
	}

	return ['success' => true, 'message' => count($new) . " new domain(s), " . count($missing) . " retired domain(s) applied for $pigeonMail."];
}

function getDomainSequencePosition($local, $jobId, $pigeonMail, $domainName) {
	$jobId = (int) $jobId;
	$pigeonMailEsc = $local->real_escape_string($pigeonMail);
	$domainNameEsc = $local->real_escape_string($domainName);

	$sql = "SELECT domain_position
			FROM seed_rotation_domain_sequence
			WHERE job_id = $jobId AND pigeon_mail = '$pigeonMailEsc' AND domain_name = '$domainNameEsc' AND is_active = '1'
			LIMIT 1";
	$res = $local->query($sql);
	$row = $res->fetch_assoc();

	return (int) $row['domain_position'];
}

function getMaxDomainPosition($local, $jobId, $pigeonMail) {
	$jobId = (int) $jobId;
	$pigeonMailEsc = $local->real_escape_string($pigeonMail);

	$sql = "SELECT MAX(domain_position) AS max_position
			FROM seed_rotation_domain_sequence
			WHERE job_id = $jobId AND pigeon_mail = '$pigeonMailEsc' AND is_active = '1'";
	$res = $local->query($sql);
	$row = $res->fetch_assoc();

	return $row['max_position'] !== null ? (int) $row['max_position'] : 0;
}

function insertDomainSequence($local, $jobId, $pigeonMail, $domainName, $position) {
	$jobId = (int) $jobId;
	$pigeonMailEsc = $local->real_escape_string($pigeonMail);
	$domainNameEsc = $local->real_escape_string($domainName);
	$position = (int) $position;

	// if domain is retired before, activate back
	$sqlExisting = "SELECT id
			FROM seed_rotation_domain_sequence
			WHERE job_id = $jobId AND pigeon_mail = '$pigeonMailEsc' AND domain_name = '$domainNameEsc' AND is_active = '0'
			LIMIT 1";
	$resExisting = $local->query($sqlExisting);

	if ($resExisting && $resExisting->num_rows > 0) {
		$sql = "UPDATE seed_rotation_domain_sequence
				SET is_active = '1', domain_position = $position
				WHERE job_id = $jobId AND pigeon_mail = '$pigeonMailEsc' AND domain_name = '$domainNameEsc' AND is_active = '0'";

		return $local->query($sql);
	}

	$sql = "INSERT INTO seed_rotation_domain_sequence (job_id, pigeon_mail, domain_name, domain_position, is_active)
			VALUES ($jobId, '$pigeonMailEsc', '$domainNameEsc', $position, '1')";

	return $local->query($sql);
}

function retireDomainSequence($local, $jobId, $pigeonMail, $domainName) {
	$jobId = (int) $jobId;
	$pigeonMailEsc = $local->real_escape_string($pigeonMail);
	$domainNameEsc = $local->real_escape_string($domainName);

	$sql = "UPDATE seed_rotation_domain_sequence 
			SET is_active = '0', updated_datetime = current_timestamp
			WHERE job_id = $jobId AND pigeon_mail = '$pigeonMailEsc' AND domain_name = '$domainNameEsc' AND is_active = '1'";

	return $local->query($sql);
}

function getGroupDomain($local, $jobId, array $serverOrder) {
	$jobId = (int) $jobId;

	$serverDomain = [];
	foreach ($serverOrder as $pigeonMail) {
		$pigeonMailEsc = $local->real_escape_string($pigeonMail);

		$sql = "SELECT pigeon_mail, domain_name, domain_position
				FROM seed_rotation_domain_sequence
				WHERE job_id = $jobId AND pigeon_mail = '$pigeonMailEsc' AND is_active = '1'
				ORDER BY domain_position ASC";
		$res = $local->query($sql);

		while ($row = $res->fetch_assoc()) {
			$serverDomain[] = [
				'pigeon_mail' => $row['pigeon_mail'],
				'domain_name' => $row['domain_name'],
			];
		}
	}

	return $serverDomain;
}

function getSeedInfoById($ext, $seedId) {
	$seedId = (int) $seedId;

	$sql = "SELECT seed_email, seed_pass, seed_domain, seed_status, seed_statdate, seed_server, seed_remark
			FROM seed_info
			WHERE seed_id = $seedId
			LIMIT 1";
	$res = $ext->query($sql);

	return $res ? $res->fetch_assoc() : null;
}

function insertSeedInfoToDestination($destinationExt, array $sourceSeedInfo, $destinationPigeonMail) {
	$emailEsc = $destinationExt->real_escape_string($sourceSeedInfo['seed_email']);
	$passEsc = $destinationExt->real_escape_string($sourceSeedInfo['seed_pass']);
	$domainEsc = $destinationExt->real_escape_string($sourceSeedInfo['seed_domain']);
	$statusEsc = $destinationExt->real_escape_string($sourceSeedInfo['seed_status']);
	$statdateEsc = $destinationExt->real_escape_string($sourceSeedInfo['seed_statdate']);
	$remarkEsc = $destinationExt->real_escape_string($sourceSeedInfo['seed_remark']);
	$serverEsc = $destinationExt->real_escape_string($sourceSeedInfo['seed_server']);

	$sql = "INSERT INTO seed_info (seed_email, seed_pass, seed_domain, seed_status, seed_statdate, seed_server, seed_remark)
			VALUES ('$emailEsc', '$passEsc', '$domainEsc', '$statusEsc', '$statdateEsc', '$serverEsc', '$remarkEsc')
			ON DUPLICATE KEY UPDATE seed_id = LAST_INSERT_ID(seed_id)";

	if (!$destinationExt->query($sql)) {
		return ['success' => false, 'error' => $destinationExt->error];
	}

	return ['success' => true, 'seed_id' => $destinationExt->insert_id];
}

function deleteSeedInfoFromSource($sourceExt, $sourceSeedId) {
	$sourceSeedId = (int) $sourceSeedId;

	$sql = "DELETE FROM seed_info WHERE seed_id = $sourceSeedId";

	if (!$sourceExt->query($sql)) {
		return ['success' => false, 'error' => $sourceExt->error];
	}

	return ['success' => true];
}

function getSeedProxyById($ext, $seedId) {
	$seedId = (int) $seedId;

	$sql = "SELECT proxy_type, proxy_value
			FROM seed_proxy
			WHERE seed_id = $seedId
			LIMIT 1";
	$res = $ext->query($sql);

	if (!$res || $res->num_rows === 0) {
		return null; // in case no proxy found for seed
	}

	return $res->fetch_assoc();
}

function insertSeedProxyToDestination($destinationExt, array $sourceSeedProxy, $destinationSeedId) {
	$destinationSeedId = (int) $destinationSeedId;
	$proxyTypeEsc = $destinationExt->real_escape_string($sourceSeedProxy['proxy_type']);
	$proxyValueEsc = $destinationExt->real_escape_string($sourceSeedProxy['proxy_value']);

	$sql = "INSERT INTO seed_proxy (seed_id, proxy_type, proxy_value)
			VALUES ($destinationSeedId, '$proxyTypeEsc', '$proxyValueEsc')
			ON DUPLICATE KEY UPDATE proxy_type = VALUES(proxy_type), proxy_value = VALUES(proxy_value)";

	if (!$destinationExt->query($sql)) {
		return ['success' => false, 'error' => $destinationExt->error];
	}

	return ['success' => true];
}

function deleteSeedProxyFromSource($sourceExt, $sourceSeedId) {
	$sourceSeedId = (int) $sourceSeedId;

	$sql = "DELETE FROM seed_proxy WHERE seed_id = $sourceSeedId";

	if (!$sourceExt->query($sql)) {
		return ['success' => false, 'error' => $sourceExt->error];
	}

	return ['success' => true];
}
?>