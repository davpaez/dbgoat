from functools import reduce
import operator
import subprocess
from typing import Union


def getDictItem(path: str, data_dict):
	return reduce(operator.getitem, path.split('.'), data_dict)


def runShellCommand(command: Union[str, list], shell=False, input=None, **kwargs):
	"""Run subprocess and capture its output"""
	completed_process = subprocess.run(
		command, 
		shell=shell, 
		input=input, 
		capture_output=True, 
		**kwargs)
	
	return completed_process


def buildCommand(executable: str, options_values: dict, options_flags: dict, **kwargs):
	# Build command
	cmd_chain = [executable]

	# Combines the default options with the ones passed as arguments
	options = options_values.copy()
	options.update(kwargs)

	def flatAppend(flat_list, something):
		if isinstance(something, list):
			for item in something:
				if isinstance(item, list):
					flatAppend(flat_list, item)
				else:
					flat_list.append(item)
		else:
			flat_list.append(something)
	
	# Compose default commands
	for key, value in options.items():
		if key in options_flags.keys():
			if value:
				cmd_chain.append(f'{options_flags[key]}={value}')
			else:
				cmd_chain.append(options_flags[key])
		elif key.startswith('__'):
			key = key.replace('_', '-')
			if value is not None:
				cmd_chain.append(f'{key}={value}')
			else:
				cmd_chain.append(key)
		else:
			flatAppend(cmd_chain, value)
	
	return cmd_chain
