# content of test_sample.py
from scicat_bot import ScicatBot


def test_answer():
    """test"""
    bot = ScicatBot()
    assert(isinstance(bot.base_url, str))
