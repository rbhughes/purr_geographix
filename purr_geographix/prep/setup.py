import ctypes
import winreg


# YOU MUST RUN THIS SCRIPT AS LOCAL ADMIN to write to the registry!
# YOU MUST HAVE PYTHON IN THE PATH SO THAT CMD RECOGNIZES IT
# There are no external dependencies, no need to set venv for these steps:
#
# 1. right-click Command Prompt | Run as administrator
# 2. python setup.py


def print_key_info(key_base, key_path):
    try:
        # Open the registry key
        key = winreg.OpenKey(key_base, key_path, 0, winreg.KEY_READ)

        # Get key information
        num_subkeys, num_values, last_modified = winreg.QueryInfoKey(key)

        print(f"Key: {key_path}")
        print(f"Number of subkeys: {num_subkeys}")
        print(f"Number of values: {num_values}")
        print(f"Last modified: {last_modified}")
        print("\nValues:")

        # Enumerate and print all values
        for i in range(num_values):
            name, data, type = winreg.EnumValue(key, i)
            print(f"  {name}: {data} (Type: {type})")

        print("\nSubkeys:")

        # Enumerate and print all subkeys
        for i in range(num_subkeys):
            subkey_name = winreg.EnumKey(key, i)
            print(f"  {subkey_name}")

        # Close the key
        winreg.CloseKey(key)

    except WindowsError as e:
        print(f"Error accessing registry key: {e}")


###############################################################################


def register_du() -> None:
    hkcu = winreg.ConnectRegistry(None, winreg.HKEY_CURRENT_USER)
    key_name = r"SOFTWARE\Sysinternals\Du"
    du_key = winreg.CreateKey(hkcu, key_name)

    print(f"Installing {key_name}")

    winreg.SetValueEx(du_key, "EulaAccepted", 0, winreg.REG_DWORD, 1)
    print_key_info(hkcu, key_name)


def is_admin():
    """returns 1 if admin, 0 if not"""
    x = ctypes.windll.shell32.IsUserAnAdmin()
    return x > 0


def prepare():
    if is_admin():
        register_du()
    else:
        print("YOU MUST RUN THIS SCRIPT AS ADMIN to write to the registry!")
        print("YOU MUST HAVE PYTHON IN THE PATH SO THAT CMD RECOGNIZES IT")
        print("1. right-click Command Prompt | Run as administrator")
        print("2. python setup.py")

        # no luck getting this scheme to work, just warn to use Admin...
        # ctypes.windll.shell32.ShellExecuteW(
        #     None, "runas", sys.executable, " ".join(sys.argv), None, 1
        # )
