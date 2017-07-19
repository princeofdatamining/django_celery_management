from django.conf import settings
from django_celery_beat.models import *

import sys
import os
import re
import json
try:
    import yaml
except:
    pass


def _datetime_to_str(value):
    if not value:
        return 'Never'
    s = value.isoformat()
    s = re.sub(r'\+00\:00$', 'Z', s)
    s = re.sub(r'(\.\d{3})\d+', r'\1', s)
    return s


def list_task():
    """ List all tasks.
    """
    print('#-{:3s}-{:40}-Runs-{:17s}-#'.format(' ID', 'Task', 'Last Run At'))
    for row in PeriodicTask.objects.all().order_by('-enabled', 'id'):
        print('{} {:4d} {:40} {:4d} {} {}'.format(
            'Y' if row.enabled else 'X',
            row.id,
            row.task[:40],
            row.total_run_count,
            _datetime_to_str(row.last_run_at),
            str(row.crontab) if row.crontab_id else str(row.interval),
        ))
    print('{}-{}-{}-{}-{}'.format('#', '-'*3, '-'*40, '-'*4, '-'*17, '#'))


def get_task(**kwargs):
    """ List task detail
    """
    print('-'*32)
    for row in PeriodicTask.objects.filter(**kwargs):
        print('{:12s} {}'.format('ID:', row.id))
        print('{:12s} {}'.format('Enabled:', row.enabled))
        print('{:12s} {}'.format('Name:', row.name))
        if row.description: print('{:12s} {}'.format('Description:', row.description))
        print('{:12s} {}'.format('Task:', row.task))
        if row.args != '[]': print('{:12s} {}'.format('args:', row.args))
        if row.kwargs != '{}': print('{:12s} {}'.format('kwargs:', row.kwargs))
        print('{:12s} {}'.format('Runs:', row.total_run_count))
        print('{:12s} {}'.format('LastRun:', _datetime_to_str(row.last_run_at)))
        if row.crontab_id: print('{:12s} {}'.format('Crontab:', str(row.crontab)))
        if row.interval_id: print('{:12s} {}'.format('Interval:', str(row.interval)))
        print('-'*32)
    print()


CRONTAB_KWARGS = dict(minute='*',
                      hour='*',
                      day_of_week='*',
                      day_of_month='*',
                      month_of_year='*')


"""
-
    name: "Sync Data"
    task: "app_foo.tasks.task_foo"
    kwargs:
        id: "..."
    crontab:
        minute: 10
-
    name: "Daily Count"
    task: "app_bar.tasks.task_bar"
    crontab:
        minute: 0
        hour: 1
-
    name: "test"
    task: "foo.bar"
    enabled: false
    interval:
        seconds: 10
"""
def load_tasks(filename):
    if not filename:
        if not hasattr(settings, 'CELERY_BEAT_CONFIG'):
            print('no CELERY_BEAT_CONFIG in settings.py')
            sys.exit(1)
        filename = settings.CELERY_BEAT_CONFIG
    if not filename:
        print('no filename')
        sys.exit(1)
    if not filename.startswith('/'):
        filename = os.path.join(settings.BASE_DIR, filename)
    if not os.path.isfile(filename):
        print('filename `{}` not exist'.format(filename))
        sys.exit(1)
    with open(filename, encoding='utf-8') as fp:
        if filename.endswith('.yml'):
            tasks = yaml.load(fp)
        elif filename.endswith('.json'):
            tasks = json.load(fp)
        else:
            print('Unsupport file extension')
            sys.exit(1)
    for data in tasks:
        # 调度方法
        task = data.get('task')
        if not task:
            print('no task in {}'.format(data))
            continue
        # 任务名
        name = data.get('name') or task
        try:
            instance = PeriodicTask.objects.get(name=name)
        except PeriodicTask.DoesNotExist:
            instance = PeriodicTask(name=name, task=task)
        update_fields = [
            'task', 'enabled', 'description', 'args', 'kwargs',
            'crontab_id', 'interval_id']
        original = {k: getattr(instance, k) for k in update_fields}
        # 默认启动
        instance.enabled = data.get('enabled', True)
        # 任务描述
        if 'description' in data:
            instance.description = data['description']
        elif not instance.pk:
            instance.description = name
        #
        args = data.get('args')
        if args is not None:
            if isinstance(args, str):
                instance.args = args
            elif isinstance(args, list):
                instance.args = json.dumps(args)
            else:
                print('args must be str or list, but got: {}'.format(args))
                continue
        elif not instance.pk:
            instance.args = '[]'
        #
        kwargs = data.get('kwargs')
        if kwargs is not None:
            if isinstance(kwargs, str):
                instance.kwargs = kwargs
            elif isinstance(kwargs, dict):
                instance.kwargs = json.dumps(kwargs)
            else:
                print('kwargs must be str or dict, but got: {}'.format(kwargs))
                continue
        elif not instance.pk:
            instance.kwargs = '{}'
        #
        if 'crontab' in data:
            kwargs = CRONTAB_KWARGS.copy()
            kwargs.update(data['crontab'])
            if not data['crontab']:
                instance.enabled = False
            else:
                instance.crontab, _ = CrontabSchedule.objects.get_or_create(**kwargs)
                instance.interval = None
        elif 'interval' in data:
            kwargs = dict()
            for period, every in data['interval'].items():
                kwargs['every'] = every
                kwargs['period'] = period
                instance.interval, _ = IntervalSchedule.objects.get_or_create(**kwargs)
                instance.crontab = None
                # 只能有一条有效值
                break
            # 没有设置interval，则停止该task
            if not instance.interval:
                instance.enabled = False
        else:
            instance.enabled = False
        # 是否需要保存
        update_fields = [
            k
            for k, v in original.items()
            if v != getattr(instance, k)
        ]
        if not instance.pk:
            print('add task:', task)
            instance.save()
        elif update_fields:
            print('set task:', task, update_fields)
            instance.save(update_fields=update_fields)
    #
