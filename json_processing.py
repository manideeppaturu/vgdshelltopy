import json

class JSONProcess():
	def __init__(self):
		self.__data=None
	
	def readJsonFile(self,file_name):
		with open (file_name) as json_file:
			self.__data=json.load(json_file)

	def json_processing(self,tag_value):
		for tag in self.__data['employees']:
			if tag['name']==tag_value:
				return tag
	def close(self):
		pass
			

