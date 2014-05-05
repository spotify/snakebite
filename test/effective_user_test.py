from minicluster_testbase import MiniClusterTestBase
from snakebite.client import Client
import os

class EffectiveUserTest(MiniClusterTestBase):
    ERR_MSG_TOUCH = "org.apache.hadoop.security.AccessControlException\nPermission denied: user=__foobar"
    ERR_MSG_STAT = "`/foobar2': No such file or directory"

    VALID_FILE = '/foobar'
    INVALID_FILE = '/foobar2'

    def setUp(self):
        self.custom_client = Client(self.cluster.host, self.cluster.port)
        self.custom_foobar_client = Client(host=self.cluster.host,
                                           port=self.cluster.port,
                                           effective_user='__foobar')

    def test_touch(self):
        print tuple(self.custom_client.touchz([self.VALID_FILE]))
        try:
            tuple(self.custom_foobar_client.touchz([self.INVALID_FILE]))
	except Exception, e:
            self.assertTrue(e.message.startswith(self.ERR_MSG_TOUCH))

        self.custom_client.stat([self.VALID_FILE])
        try:
            self.custom_client.stat([self.INVALID_FILE])
        except Exception, e:
            self.assertEquals(e.message, self.ERR_MSG_STAT)
