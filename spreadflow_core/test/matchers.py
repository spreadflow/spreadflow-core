from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from testtools import matchers

class MatchesInvocation(matchers.MatchesListwise):
    """
    Matches an invocation recorded by :class:`unittest.mock.Mock.call_args`

    Args:
        *args: Matchers matching the recorded parameter at the same position.
        **kwds: Matchers matching the recorded keyword parameter with the same
            key.
    """
    def __init__(self, *args, **kwds):
        super(MatchesInvocation, self).__init__([
            matchers.MatchesListwise(args),
            matchers.MatchesDict(kwds)
        ])
