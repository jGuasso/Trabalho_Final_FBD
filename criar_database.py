import requests
from bs4 import BeautifulSoup
import mysql.connector

host = input("Host:")
user = input("User:")
password = input("Senha:")

# 1. Conexão com o MySQL
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
)
cursor = conn.cursor()

cursor.execute("DROP DATABASE IF EXISTS olimpiadas_br")
cursor.execute("CREATE DATABASE olimpiadas_br")
cursor.execute("USE olimpiadas_br")


#Tabela com as modalidades
cursor.execute("""
CREATE TABLE IF NOT EXISTS modalidade (
    idModalidade INT NOT NULL AUTO_INCREMENT,
    esporte VARCHAR(255) NOT NULL,
    modalidade VARCHAR(255) NOT NULL,
    PRIMARY KEY (idModalidade)
)
""")

#Tabela com todas as medalhas
cursor.execute("""
CREATE TABLE IF NOT EXISTS medalha (
    idMedalha INT NOT NULL AUTO_INCREMENT,
    edicao VARCHAR(255) NOT NULL,
    medalha VARCHAR(10) NOT NULL,
    idModalidade INT NOT NULL,
        FOREIGN KEY (idModalidade)
        REFERENCES modalidade (idModalidade),
    PRIMARY KEY (idMedalha)
)
""")

#Tabela com todos atletas
cursor.execute("""
CREATE TABLE IF NOT EXISTS atleta (
    idAtleta INT NOT NULL AUTO_INCREMENT,
    nome VARCHAR(255) NOT NULL,
    PRIMARY KEY (idAtleta)    
)
""")

#Conecta o Atleta e a medalha
cursor.execute("""
CREATE  TABLE elenco (
  idMedalha INT NOT NULL ,
  idAtleta INT NOT NULL ,
  PRIMARY KEY (idMedalha, idAtleta) ,
  INDEX fk_elenco_medalha_idx (idMedalha ASC) ,
  INDEX fk_elenco_atleta_idx (idAtleta ASC) ,
  CONSTRAINT fk_elenco_medalha
    FOREIGN KEY (idMedalha)
    REFERENCES medalha (idMedalha),
  CONSTRAINT fk_elenco_atleta
    FOREIGN KEY (idAtleta)
    REFERENCES atleta (idAtleta));
""")

#Acessa a página da Wikipédia
url = "https://pt.wikipedia.org/wiki/Lista_de_medalhas_brasileiras_nos_Jogos_Ol%C3%ADmpicos"
html = requests.get(url).text
soup = BeautifulSoup(html, "html.parser")

inicio = soup.find('h2', id='Medalhistas')#onde começa a procurar
fim = soup.find('h2', id='Medalhas_por_edição')#onde para de procurar
edicao_filtro = soup.find_all('div',class_='mw-heading3')#filtra todos os possíveis nomes de edição
tabela_filtro = soup.find_all('table')

# Pega tudo depois de "Inicio"
elementos_depois = inicio.find_all_next()
# Agora filtra até encontrar o "Fim"
for elem in elementos_depois:
    if elem == fim:
        break
    #Verifica se é o texto de uma edição
    if elem in edicao_filtro:
        edicao=elem.find('h3').text
    #Verifica se é uma tabela
    if elem in tabela_filtro:
        #único jeito que consegui filtrar sem o cabeçalho
        linhas = elem.find_all('tr',bgcolor="#efefef")
        for linha in linhas:
            celulas = linha.find_all('td')
            dados = [c.get_text(strip=True) for c in celulas]
            cursor.execute("""SELECT mo.idModalidade
                      FROM modalidade mo
                      where mo.esporte = %s and mo.modalidade = %s
                   """,(dados[2],dados[3]))
            res = cursor.fetchone()

            if res:
                idModalidade = res[0]
            else:
                cursor.execute("INSERT INTO modalidade (esporte, modalidade) VALUES (%s, %s)",(dados[2],dados[3]))
                idModalidade = cursor.lastrowid

            cursor.execute("""
                INSERT INTO medalha  (edicao, medalha, idModalidade)
                VALUES (%s, %s, %s)
            """, (edicao, dados[0],idModalidade))
            idMedalha = cursor.lastrowid  # pega o ID recém-criado
            #Separa o nome de todos os atletas
            nomes = [a.get_text(strip=True) for a in celulas[1].find_all("a")]
            #Retira as referências da wikipédia como [1]
            nomes = [n for n in nomes if not n.startswith('[') and not n.endswith(']')]

            for nome in nomes:
                # Verifica se o nome já está no banco
                if "Seleção" in nome:
                    continue
                cursor.execute("SELECT idAtleta FROM atleta WHERE nome = %s", (nome,))
                res = cursor.fetchone()

                if res:
                    idAtleta = res[0]
                else:
                    cursor.execute("INSERT INTO atleta (nome) VALUES (%s)", (nome,))
                    idAtleta = cursor.lastrowid

                cursor.execute("""
                    INSERT INTO elenco (idMedalha, idAtleta)
                    VALUES (%s, %s)
                """, (idMedalha, idAtleta))


            

conn.commit()
cursor.close()
conn.close()