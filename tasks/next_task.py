# -*- coding: utf-8 -*-


def determine_next_task( current_task ):
  """ Returns next task. """
  next_task = None
  if current_task == u'check_redis_status_dict':
    next_task = u'tasks.check_environment.archive_previous_work'
  elif current_task == u'archive_previous_work':
    next_task = u'tasks.check_environment.check_main_accession_number_dict'
  return next_task
