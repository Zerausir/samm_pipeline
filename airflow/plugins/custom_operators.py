from airflow.sdk.bases.operator import BaseOperator
import subprocess
import os


class WindowsServiceOperator(BaseOperator):
    """
    Operador personalizado para iniciar, detener o reiniciar servicios de Windows

    :param service_name: Nombre del servicio de Windows
    :param action: Acción a realizar (start, stop, restart)
    """

    def __init__(self, service_name, action='start', *args, **kwargs):
        super(WindowsServiceOperator, self).__init__(*args, **kwargs)
        self.service_name = service_name
        self.action = action

    def execute(self, context):
        if self.action not in ['start', 'stop', 'restart']:
            raise ValueError(f"Acción no válida: {self.action}. Use 'start', 'stop' o 'restart'")

        try:
            if self.action == 'restart':
                subprocess.run(['net', 'stop', self.service_name], check=True)
                subprocess.run(['net', 'start', self.service_name], check=True)
                self.log.info(f"Servicio {self.service_name} reiniciado con éxito")
            else:
                subprocess.run(['net', self.action, self.service_name], check=True)
                self.log.info(f"Servicio {self.service_name} {self.action}ado con éxito")

            return f"Servicio {self.service_name} {self.action}ado con éxito"
        except subprocess.CalledProcessError as e:
            self.log.error(f"Error al {self.action} el servicio {self.service_name}: {str(e)}")
            raise
