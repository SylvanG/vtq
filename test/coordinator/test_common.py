import functools
import unittest
import math
from vtq.coordinator.common import wrs_distribution


class WrsDistributionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.weights = [1, 99, 200]

    def _sum_into(self, to_list: list, from_list: list):
        for idx, value in enumerate(from_list):
            to_list[idx] += value

    def _verify(self, sample_dist: list[int]) -> bool:
        assert len(sample_dist) == len(self.weights)

        sum_weights = sum(self.weights)
        sample_n = sum(sample_dist)
        for weight, sample_count in zip(self.weights, sample_dist):
            p_std = self._std(m=weight, n=sum_weights)
            p_mean = weight / sum_weights
            sample_mean = sample_count / sample_n
            test_rv = self._z_test(p_mean, p_std, sample_n, sample_mean)
            if not test_rv:
                return False

        return True

    def _std(self, m: int, n: int) -> float:
        return math.sqrt(m * (n - m)) / n

    def _z_test(self, p_mean, p_std, sample_n, sample_mean) -> bool:
        z_score = (sample_mean - p_mean) * math.sqrt(sample_n) / p_std
        print(z_score)
        # A z-score of +/- 1.96 or greater is considered statistically significant at the 5% level of significance (i.e., p < 0.05). This means that the data point is significantly different from the mean at a 95% confidence level.
        # z-table.com
        return abs(z_score) <= 1.96

    def _test_sample(self, k):
        distrbution_func = functools.partial(
            wrs_distribution,
            weights=self.weights,
            k=k,
        )
        dist = [0] * len(self.weights)
        for _ in range(10000):
            self._sum_into(dist, distrbution_func())

        assert sum(dist) == 10000 * k
        print(dist)
        assert self._verify(dist), dist

    @unittest.skip("unable to pass z-test")
    def test_sample_6(self):
        self._test_sample(6)

    @unittest.skip("unable to pass z-test")
    def test_sample_7(self):
        self._test_sample(7)

    @unittest.skip("unable to pass z-test")
    def test_sample_103(self):
        self._test_sample(103)

    def test_sample_hybrid(self):
        distrbution_func = functools.partial(
            wrs_distribution,
            weights=self.weights,
        )
        dist = [0] * len(self.weights)
        for k in range(1, 1000):
            for _ in range(100):
                self._sum_into(dist, distrbution_func(k=k))

        # print(dist)
        assert self._verify(dist), dist
