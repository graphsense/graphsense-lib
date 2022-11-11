import sys


def info(extype, value, tb):
    if hasattr(sys, "ps1") or not sys.stderr.isatty():
        # we are in interactive mode or we don't have a tty-like
        # device, so we call the default hook
        sys.__excepthook__(extype, value, tb)
    else:
        import pdb
        import traceback

        # we are NOT in interactive mode, print the exception...
        traceback.print_exception(extype, value, tb)
        print
        # ...then start the debugger in post-mortem mode.
        pdb.post_mortem(tb)


sys.excepthook = info
