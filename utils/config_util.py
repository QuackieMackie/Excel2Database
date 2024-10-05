import configparser
import os

def load_config(config_path='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)

    for section in config.sections():
        for key, val in config.items(section):
            if val.startswith('${') and val.endswith('}'):
                env_var = val[2:-1]
                if env_var in os.environ:
                    config[section][key] = os.environ[env_var]
                else:
                    raise ValueError(f"Environment variable {env_var} not found for {key} in section {section}")

    return config
