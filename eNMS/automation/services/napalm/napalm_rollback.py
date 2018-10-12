from multiprocessing.pool import ThreadPool
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.mutable import MutableDict

from eNMS.automation.helpers import napalm_connection, NAPALM_DRIVERS
from eNMS.automation.models import Service, service_classes


class NapalmRollbackService(Service):

    __tablename__ = 'NapalmRollbackService'

    id = Column(Integer, ForeignKey('Service.id'), primary_key=True)
    has_targets = True
    driver = Column(String)
    driver_values = NAPALM_DRIVERS
    operating_system = Column(String)
    optional_args = Column(MutableDict.as_mutable(PickleType), default={})
    vendor = Column(String)

    __mapper_args__ = {
        'polymorphic_identity': 'napalm_rollback_service',
    }

    def job(self, workflow_results=None):
        targets = self.compute_targets()
        results = {'success': True, 'devices': {}}
        pool = ThreadPool(processes=len(targets))
        pool.map(self.device_job, [(device, results) for device in targets])
        pool.close()
        pool.join()
        return results

    def device_job(self, args):
        device, results = args
        try:
            napalm_driver = napalm_connection(self, device)
            napalm_driver.open()
            napalm_driver.rollback()
            napalm_driver.close()
            result, success = 'Rollback successful', True
        except Exception as e:
            result, success = f'service failed ({e})', False
            results['success'] = False
        results['devices'][device.name] = {
            'success': success,
            'result': result
        }


service_classes['napalm_rollback_service'] = NapalmRollbackService
