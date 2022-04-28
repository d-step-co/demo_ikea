from crontab import CronTab

cron = CronTab(user=True)
job = cron.new(command='python /user_path/demo_ikea/get_data.py')
job.minute.every(15)

cron.write()
