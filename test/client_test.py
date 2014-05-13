import unittest2
import inspect

from snakebite.client import HAClient

class ClientTest(unittest2.TestCase):
    def test_wrapped_methods(self):
        public_methods = [(name, method) for name, method in inspect.getmembers(HAClient, inspect.ismethod) if not name.startswith("_")]
        self.assertGreater(len(public_methods), 0)
        wrapped_methods = [str(method) for name, method in public_methods if ".wrapped" in str(method)]
        self.assertEqual(len(public_methods), len(wrapped_methods))