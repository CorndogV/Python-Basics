__author__ = 'Anthony'
import requests,csv,os
from BeautifulSoup import BeautifulSoup
from text_unidecode import unidecode

def scrape_data(url):
	url_data = request_data(url)
	return url_data

def request_data(url):
	soup = BeautifulSoup(requests.get(url).text)
	table = soup.find("table", {"class": "wikitable sortable"})
	data = []
	for row in table.findAll('tr'):
		col = row.findAll('td')
		if not col:
			continue
		player_data = []
		for i,entry in enumerate(col):
			if i == 0:
				lines = unidecode(entry.text.strip().replace(',',''))
				name = lines.split('[')[0]
				mid_elem = len(name)//2
				names = name[mid_elem:].split(' ')
				player_data.append(names[-1])
				# Now check if player has double barrel first name
				if len(names[0:-1]) >1:
					player_data.append(names[0] + ' ' + names[1])
				else:
					player_data.append(names[0])
			elif i == 3:
				lines = str(entry.text.strip().replace(',','|'))
				player_data.append(lines)
			else:
				lines = str(entry.text.strip())
				player_data.append(lines)
		data.append(player_data)
	try:
		filename = os.path.expanduser('~\\Desktop\\Gaa.csv')
	except:
		filename = os.path.expanduser('~/Desktop/Gaa.csv')
	write_to_csv(filename,data)

def write_to_csv(filename,data):
		with open(filename, 'w') as fp:
			a = csv.writer(fp, delimiter=',', lineterminator='\n')
			header = ['Last Name','First Name(s)','County','Finals Won','Years','Notes']
			data.insert(0,header)
			a.writerows(data)
		return

def main():
	url = 'http://en.wikipedia.org/wiki/List_of_All-Ireland_Senior_Football_Championship_winning_players'
	scrape_data(url)


if __name__=='__main__':
	main()