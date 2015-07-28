import os
import platform

if platform.system() != "Windows":
    import pwd
else:
    import getpass

def get_current_username():
    if platform.system() != "Windows":
        return pwd.getpwuid(os.getuid())[0]
    else:
        return getpass.getuser()