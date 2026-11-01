import unittest

from net.bootstrap import SeedAddress, parse_seed_address


class TestBootstrap(unittest.TestCase):
    def test_parse_valid_seed_address(self):
        seed = parse_seed_address("127.0.0.1:8000")

        self.assertEqual(seed.host, "127.0.0.1")
        self.assertEqual(seed.port, 8000)

    def test_parse_hostname(self):
        seed = parse_seed_address("localhost:9000")

        self.assertEqual(seed.host, "localhost")
        self.assertEqual(seed.port, 9000)

    def test_reject_empty_address(self):
        with self.assertRaises(ValueError):
            parse_seed_address("")

    def test_reject_missing_port(self):
        with self.assertRaises(ValueError):
            parse_seed_address("localhost")

    def test_reject_non_integer_port(self):
        with self.assertRaises(ValueError):
            parse_seed_address("localhost:abc")

    def test_reject_invalid_port(self):
        with self.assertRaises(ValueError):
            parse_seed_address("localhost:70000")

    def test_seed_address_is_immutable(self):
        seed = SeedAddress("localhost", 8000)

        with self.assertRaises(AttributeError):
            seed.port = 9000


if __name__ == "__main__":
    unittest.main()
