import uuid


class Item:
    def __init__(self, root: str, name: str, folderish: bool):
        self.path = root + '/' + name
        self.name = name
        self.folderish = folderish

    def set_path(self, path: str):
        self.path = path

    def get_dict(self):
        t = 'Folder' if self.folderish else 'File'
        return {
            'title': self.name,
            'path': self.path,
            'folderish': self.folderish,
            'properties': {

            },
            'hash': str(uuid.uuid1()),
            'parent': 'parent',
            'type': t,
        }
