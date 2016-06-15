#from gcSettings import Settings
#   this import is only needed to execute the script standalone
#   (<GCDIR>/packages needs to be in your PYTHONPATH - or grid-control was properly installed)

import time
print(time.time())

cfg = Settings()
cfg.workflow.task = 'UserTask'
cfg.workflow.backend = 'Host'

cfg.jobs.wall_time = '1:00'
cfg.section('jobs').jobs = 2

cfg.usertask.executable = 'Example02_local.sh'
cfg.usertask.set('arguments', 'arg1 arg2 arg3')
cfg.usertask.dataset = ['Example05_dataset.dbs', ':file:/bin/sh|3', ':file:/bin/bash|3']
cfg.usertask.set('files per job', 2)

print(cfg)
print('=' * 20)
