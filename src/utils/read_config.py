from ConfigParser import ConfigParser
from os import path
from set_working_directory import *


class ConfigRead:
    def __init__(self):
        self.config = ConfigParser()
        self.config.read(path.join(path.join(path.dirname(path.dirname(path.abspath(__file__))), 'config/config.ini')))

    def get_config_section(self, section_name):
        if not section_name:
            raise Exception("Section name is required")

        if section_name not in self.config.sections():
            raise Exception("Invalid section name")

        return dict(self.config.items(section_name))

    def get_property(self, section_name, property_name):
        section_dict = self.get_config_section(section_name)

        if not property_name:
            raise Exception("Property name is required")

        if property_name not in section_dict.keys():
            raise Exception("Invalid property name")

        return section_dict[property_name]


if __name__ == '__main__':
    config_obj = ConfigRead()
    print dummy
