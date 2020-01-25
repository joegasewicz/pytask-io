import asyncio
import pytest

from pytask_io.pytask_io import PytaskIO


class TestPytaskIO:

    def test_pytask_io(self):
        """Test the PytaskIO class"""

        pytask_io = PytaskIO()

        assert asyncio.get_running_loop() == True
