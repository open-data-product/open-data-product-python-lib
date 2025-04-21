from datetime import datetime


class TrackingDecorator(object):
    def track_time(func):
        def wrap(*args, **kwargs):
            start_time = datetime.now()

            print("\n" + func.__qualname__ + " started")

            result = func(*args, **kwargs)

            time_elapsed = datetime.now() - start_time

            print(func.__qualname__ + " finished in {}".format(time_elapsed))

            return result

        return wrap
