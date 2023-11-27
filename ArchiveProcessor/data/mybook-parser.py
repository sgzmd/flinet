from bs4 import BeautifulSoup
import csv

# Assuming the XML file is stored in '/mnt/data/mybook.xml'
file_path = 'mybook.xml'

# Read the XML file
with open(file_path, 'r') as file:
    content = file.read()

print(content[0:100])

# Parse the XML content using BeautifulSoup
soup = BeautifulSoup(content, 'html.parser')

# Find all elements with the specified classes

divs = soup.find_all('div', class_='e4xwgl-1')


def find_title(div):
    return div.find('p', class_='lnjchu-1')

def find_author(div):
    return div.find('div', class_='dey4wx-1')


books = [{'title': find_title(div).get_text(), 'author': find_author(div).get_text()} for div in divs]

print(books)

# Path for the output CSV file
csv_file_path = 'books.csv'

# Write data to CSV file
with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['title', 'author']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for book in books:
        writer.writerow(book)
