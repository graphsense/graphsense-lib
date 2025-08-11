"""Utilities for a more beautiful CMD interface"""

import os


class bcolors:
    HEADER = "\033[1;97m"
    INFO = "\033[1;97m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


try:
    rows, columns = os.get_terminal_size(0)
except OSError:  # if running as GitHub Action
    rows, columns = 24, 80


def print_separator(symbol, text, colour=None):
    left = int((columns - len(text) - 2) / 2)
    right = columns - left - len(text) - 2
    if colour:
        print(f"{colour}", end="")
    print(symbol * left, text, symbol * right)
    if colour:
        print(f"{bcolors.ENDC}", end="")


def print_line(text, status=None):
    if status == "fail":
        colour = bcolors.FAIL
    elif status == "success":
        colour = bcolors.OKGREEN
    else:
        colour = bcolors.INFO
    print_separator("=", text, colour)


def print_info(text, **args):
    print(f"{bcolors.INFO}{text}{bcolors.ENDC}", **args)


def print_success(text, **args):
    print(f"{bcolors.OKGREEN}{text}{bcolors.ENDC}")


def print_fail(text, exception=None):
    print(f"{bcolors.FAIL}{text}{bcolors.ENDC}")
    if exception:
        print(f"{bcolors.FAIL}{exception}{bcolors.ENDC}")


def print_warn(text, **args):
    print(f"{bcolors.WARNING}{text}{bcolors.ENDC}", **args)


def get_user_choice(label, options):
    if not options:
        return None

    input_message = f"Choose for {bcolors.BOLD}{label}{bcolors.ENDC}\n"
    for index, lbl in enumerate(options):
        if type(lbl) is tuple:
            _, lbl = lbl
        input_message += f"\t{bcolors.BOLD}{index}{bcolors.ENDC} {lbl}\n"
    input_message += f"\t{bcolors.BOLD}ENTER{bcolors.ENDC} to skip\n"
    input_message += "Your choice: "

    user_input = "-1"
    while user_input and (
        not user_input.isdecimal() or int(user_input) not in range(0, len(options))
    ):
        user_input = input(input_message)

    if user_input:
        result = options[int(user_input)]
        if type(result) is tuple:
            ident, _ = result
            return ident
        return result
    return None
