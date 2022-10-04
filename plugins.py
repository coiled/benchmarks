"""
Collection of useful plugins for monitoring clusters.
"""
import sys
from collections import defaultdict

import cloudpickle
from distributed.diagnostics import SchedulerPlugin

# Tell cloudpickle we want to register objects in this module by value,
# so we can send them to the scheduler without the files existing there.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


class Durations(SchedulerPlugin):
    def __init__(self):
        """Initialize the plugin"""
        self.durations = defaultdict(float)
        self.scheduler = None
        self._tracking = False
        # Big hack to trigger cloudpickle serialization for distributed < 2022.7.0
        # https://github.com/dask/distributed/pull/6466
        self.__main__ = "__main__"

    def start(self, scheduler):
        """Called on scheduler start as well as on registration time"""
        self.scheduler = scheduler
        scheduler.handlers["get_durations"] = self.get_durations
        scheduler.handlers["start_tracking_durations"] = self.start_tracking
        scheduler.handlers["stop_tracking_durations"] = self.stop_tracking

    def start_tracking(self, comm):
        self._tracking = True
        self.durations.clear()

    def stop_tracking(self, comm):
        self._tracking = False

    def transition(self, key, start, finish, *args, **kwargs):
        """On key transition to memory, update the duration data"""
        if not self._tracking:
            return

        if start == "processing" and finish == "memory":
            startstops = kwargs.get("startstops")
            if not startstops:
                return

            for ss in startstops:
                self.durations[ss["action"]] += max(ss["stop"] - ss["start"], 0)

    async def get_durations(self, comm):
        return dict(self.durations)

    def restart(self, scheduler):
        self.durations.clear()
