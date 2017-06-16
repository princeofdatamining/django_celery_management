from django import apps

from django_celery_management import constant


class django_celery_managementAppConfig(apps.AppConfig):

    name = 'django_celery_management'
    verbose_name = constant.APP

    def ready(self):
        pass
