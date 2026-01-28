import gettext
import subprocess
import locale
import logging
import os
import random
import string
import time
from pathlib import Path

from LeaptimeManager.cli_args import APP, LOCALE_DIR
from LeaptimeManager.common import DATA_LOG_DIR
from LeaptimeManager.database_rw import databackup_db
from LeaptimeManager.dataBackup_backend import UserData_backend

# i18n
locale.bindtextdomain(APP, LOCALE_DIR)
gettext.bindtextdomain(APP, LOCALE_DIR)
gettext.textdomain(APP)
_ = gettext.gettext

module_logger = logging.getLogger('LeaptimeManager.rsync_backend')

class rsync_backend():
	def __init__(self, errors) -> None:
		module_logger.info("Initializing rsync backend class...")
		self.errors = errors
		self.db_manager = databackup_db()
		self.manager = UserData_backend(self.errors)
		self.operating = self.manager.operating
		self.data_db_list = self.db_manager.read_db()

	def build_rsync_patterns(self, included_files=None, included_dirs=None,
							 excluded_files=None, excluded_dirs=None):
		patterns = []
		seen_dirs = set()

		included_files = included_files or []
		included_dirs = included_dirs or []
		excluded_files = excluded_files or []
		excluded_dirs = excluded_dirs or []

		def add_parents(path):
			parts = Path(path).parts
			for i in range(1, len(parts)):
				parent = os.path.join(*parts[:i]) + '/'
				if parent not in seen_dirs:
					patterns.extend(['--include', parent])
					seen_dirs.add(parent)

		for idir in included_dirs:
			idir = idir.rstrip('/') + '/'
			add_parents(idir)
			if idir not in seen_dirs:
				patterns.extend(['--include', idir])
				seen_dirs.add(idir)

		for ifile in included_files:
			add_parents(ifile)
			patterns.extend(['--include', ifile])

		for edir in excluded_dirs:
			edir = edir.rstrip('/') + '/'
			patterns.extend(['--exclude', edir])

		for efile in excluded_files:
			patterns.extend(['--exclude', efile])

		if included_files or included_dirs:
			patterns.extend(['--exclude', '*'])

		return patterns

	def generate_rsync_command(self, source_dir, dest_dir, included_files, included_dirs,
							   excluded_files, excluded_dirs,
							   dry_run=False, show_progress=False, delete_extra=False):
		rsync_cmd = ['rsync', '--archive', '--acls', '--xattrs', '--hard-links', '--times', '--atimes', '--checksum', '--compress', '--partial']

		if dry_run:
			rsync_cmd.append('--dry-run')
		if show_progress:
			rsync_cmd.append('--progress')
		if delete_extra:
			rsync_cmd.append('--delete')

		rsync_cmd.extend(self.build_rsync_patterns(
			included_files=included_files,
			included_dirs=included_dirs,
			excluded_files=excluded_files,
			excluded_dirs=excluded_dirs
		))

		source_dir = os.path.abspath(source_dir)
		if not source_dir.endswith('/'):
			source_dir += '/'
		dest_dir = os.path.abspath(dest_dir)

		rsync_cmd.extend([source_dir, dest_dir])
		return rsync_cmd

	def prep_rsync_backup(self, backup_name, source_dir, dest_dir,
						  excluded_files, excluded_dirs,
						  included_files, included_dirs,
						  dry_run=False, show_progress=False, delete_extra=False, repeat=False):
		self.repeat = repeat
		self.uuid = ''.join(random.choice(string.digits + string.ascii_letters) for _ in range(8))
		self.backup_name = backup_name
		self.source_dir = source_dir
		self.dest_dir = dest_dir
		self.excluded_files = excluded_files
		self.excluded_dirs = excluded_dirs
		self.included_files = included_files
		self.included_dirs = included_dirs

		time_now = time.localtime()
		self.timestamp = time.strftime("%Y-%m-%d_%H-%M", time_now)
		backuplogdir = os.path.join(DATA_LOG_DIR, backup_name)
		Path(backuplogdir).mkdir(parents=True, exist_ok=True)
		self.backup_logfile = os.path.join(backuplogdir, f"{backup_name}_{self.timestamp}.log")

		# get a count of all the files
		self.operating = True
		self.num_files = 0
		self.total_size = 0
		self.copy_files, self.num_files, self.total_size = self.manager.scan_dirs(
			self.operating,
			self.source_dir,
			self.excluded_files,
			self.excluded_dirs,
			self.included_files,
			self.included_dirs,
			self.manager.callback_count_total
		)

		module_logger.debug("Number of files: %s, Total size in byte: %s" % (self.num_files, self.total_size))
		module_logger.debug("List of files to copy: %s" % "\n".join(self.copy_files))

		return {
			"cmd": self.generate_rsync_command(
				self.source_dir, self.dest_dir,
				self.included_files, self.included_dirs,
				self.excluded_files, self.excluded_dirs,
				dry_run=dry_run, show_progress=show_progress, delete_extra=delete_extra
			),
			"logfile": self.backup_logfile,
			"uuid": self.uuid,
			"timestamp": self.timestamp,
			"source": self.source_dir,
			"destination": self.dest_dir,
			"name": self.backup_name
		}
	
	def prep_sync_backup(self, backup_name, source_dir, dest_dir,
						 excluded_files, excluded_dirs,
						 included_files, included_dirs,
						 dry_run=False, show_progress=False, repeat=False):
		"""Prepare a sync mode backup that keeps destination in sync with source."""
		module_logger.info("Preparing sync mode backup...")
		# Sync mode uses delete_extra=True to remove files in destination that don't exist in source
		return self.prep_rsync_backup(
			backup_name, source_dir, dest_dir,
			excluded_files, excluded_dirs,
			included_files, included_dirs,
			dry_run=dry_run, show_progress=show_progress,
			delete_extra=True, repeat=repeat
		)

	def prep_incremental_backup(self, backup_name, source_dir, dest_dir,
								excluded_files, excluded_dirs,
								included_files, included_dirs,
								previous_backup_dest=None,
								dry_run=False, show_progress=False, repeat=False):
		"""Prepare an incremental backup using rsync's hard-link feature."""
		module_logger.info("Preparing incremental mode backup...")

		self.repeat = repeat
		self.uuid = ''.join(random.choice(string.digits + string.ascii_letters) for _ in range(8))
		self.backup_name = backup_name
		self.source_dir = source_dir
		self.base_dest_dir = dest_dir
		self.excluded_files = excluded_files
		self.excluded_dirs = excluded_dirs
		self.included_files = included_files
		self.included_dirs = included_dirs
		self.previous_backup_dest = previous_backup_dest

		time_now = time.localtime()
		self.timestamp = time.strftime("%Y-%m-%d_%H-%M", time_now)

		# Create incremental backup directory with timestamp
		self.dest_dir = os.path.join(self.base_dest_dir, f"{backup_name}_{self.timestamp}")
		Path(self.dest_dir).mkdir(parents=True, exist_ok=True)

		backuplogdir = os.path.join(DATA_LOG_DIR, backup_name)
		Path(backuplogdir).mkdir(parents=True, exist_ok=True)
		self.backup_logfile = os.path.join(backuplogdir, f"{backup_name}_{self.timestamp}.log")

		# get a count of all the files
		self.operating = True
		self.num_files = 0
		self.total_size = 0
		self.copy_files, self.num_files, self.total_size = self.manager.scan_dirs(
			self.operating,
			self.source_dir,
			self.excluded_files,
			self.excluded_dirs,
			self.included_files,
			self.included_dirs,
			self.manager.callback_count_total
		)

		module_logger.debug("Number of files: %s, Total size in byte: %s" % (self.num_files, self.total_size))

		# Build rsync command with incremental support
		rsync_cmd = ['rsync', '--archive', '--acls', '--xattrs', '--hard-links', '--times', '--atimes', '--checksum', '--compress', '--partial']

		if dry_run:
			rsync_cmd.append('--dry-run')
		if show_progress:
			rsync_cmd.append('--progress')

		# Add link-dest for hard-link based incremental backup
		if previous_backup_dest and os.path.exists(previous_backup_dest):
			rsync_cmd.extend(['--link-dest', os.path.abspath(previous_backup_dest)])
			module_logger.info(f"Using link-dest: {previous_backup_dest}")

		rsync_cmd.extend(self.build_rsync_patterns(
			included_files=self.included_files,
			included_dirs=self.included_dirs,
			excluded_files=self.excluded_files,
			excluded_dirs=self.excluded_dirs
		))

		source_dir_abs = os.path.abspath(self.source_dir)
		if not source_dir_abs.endswith('/'):
			source_dir_abs += '/'

		rsync_cmd.extend([source_dir_abs, self.dest_dir])

		return {
			"cmd": rsync_cmd,
			"logfile": self.backup_logfile,
			"uuid": self.uuid,
			"timestamp": self.timestamp,
			"source": self.source_dir,
			"destination": self.dest_dir,
			"name": self.backup_name,
			"mode": "incremental",
			"parent_backup": previous_backup_dest
		}

	def finish_rsync_backup(self, desc="", backup_method="rsync", backup_mode="backup", parent_backup=None):
		self.backup_method = backup_method
		self.backup_desc = desc
		self.backup_mode = backup_mode
		self.parent_backup = parent_backup

		try:
			try:
				data_backup_dict = {
					"uuid" : self.uuid,
					"name" : self.backup_name,
					"method" : self.backup_method,
					"mode" : self.backup_mode,
					"source" : self.source_dir,
					"destination" : self.dest_dir,
					"created" : self.timestamp,
					"repeat" : self.repeat,
					"comment" : self.backup_desc,
					"exclude" : (self.excluded_dirs, self.excluded_files),
					"include" : (self.included_dirs, self.included_files),
					"logfile" : self.backup_logfile,
					"count" : self.num_files,
					"size" : self.total_size,
					}

				# Add parent backup reference for incremental backups
				if self.backup_mode == "incremental" and self.parent_backup:
					data_backup_dict["parent_backup"] = self.parent_backup

				self.data_db_list.append(data_backup_dict)
				self.db_manager.write_db(self.data_db_list)
			except Exception as detail:
				print(detail)
				self.errors.append([str(detail), None])

			if self.archived_files < self.num_files:
				self.errors.append([_("Warning: Some files were not saved. Only %(archived)d files were backed up out of %(total)d.") % {'archived': self.archived_files, 'total': self.num_files}, None])

		except Exception as e:
			print(e)
