from json_processing import JSONProcess
import pytest

@pytest.fixture(scope='module')
def json_dt():
	print("Reading JSON file ")
	json_dt=JSONProcess()
	json_dt.readJsonFile('json_data.json')
	yield json_dt
	print("Colsing File ")
	json_dt.close()

def test_ABC_data(json_dt):
	ABC_data=json_dt.json_processing('ABC')
	assert ABC_data['Company']=='ASD'
	assert ABC_data['citi']=='HYD'

def test_eww_data(json_dt):
	eww_data=json_dt.json_processing('eww')
	assert eww_data['Company']=='XTY'
	assert eww_data['citi']=='MU'

