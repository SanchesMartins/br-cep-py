
# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
# import pandas as pd
import logging
import pandas as pd
from time import perf_counter, sleep

# ********************** set logger
logger = logging.getLogger("EXTRACAO_CEP")
logger.setLevel(logging.INFO)

hand = logging.StreamHandler()
formatter = logging.Formatter(
    "[%(levelname)s] %(name)s - %(asctime)s: %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
hand.setFormatter(formatter)

logger.addHandler(hand)
# ********************** set logger


def __remove_files(file_name: str) -> bool:
    """
    Remove o arquivo de um caminho especificado

    :param file_name: Nome do arquivo
    :return: Booleano indicando se o arquivo foi removido (True) ou não (False)
    :rtype: bool
    """
    from os.path import isfile, exists
    from os import remove

    if exists(file_name) and isfile(file_name):
        remove(file_name)
        if not exists(file_name):
            logger.info(f"Arquivo {file_name} removido")
            return True
        else:
            logger.error(f"Arquivo {file_name} nao pode ser removido")
            return False


def __remove_directory(dir_name: str, recursively: bool=False) -> bool:
    """
    Remove um diretorio fornecido

    :param dir_name: Caminho do diretório alvo
    :type dir_name: str

    :param recursively: Remove arquivos e pastas internas do diretorio alvo
    :type recursively: bool

    :return: Booleano indicando se o diretorio foi removido (True) ou não (False)
    :rtype: bool

    :rise FileExistsError: Se o diretorio estiver cheio e a remoção não for recursiva
    
    """
    from os import listdir, chmod
    from os.path import exists
    from shutil import rmtree
    import stat

    if exists(dir_name):
        sz = len(listdir(dir_name))
        if (sz == 0) or (sz > 0 and recursively):
            chmod(dir_name, stat.S_IWRITE)
            rmtree(dir_name)
        else:
            raise FileExistsError(f"Directory {dir_name} is not empty")

        if not exists(dir_name):
            logger.info(f"Diretorio {dir_name} removido")
            return True
        else:
            logger.info(f"Diretorio {dir_name} nao pode ser removido")
            return False

        
def list_dir(directory: str, recursively: bool=False, raise_not_exists: bool=False) -> list:
    """
    Lista o conteúdo de um diretorio

    :param directory: Diretorio alvo
    :type directory: str

    :param recursively: Lista recursivamente
    :type recursively: bool, padrão False

    :param raise_not_exists: Lança um erro se o diretorio alvo não existir
    :type raise_not_exists: bool, padrão False
    """

    from os import listdir
    from os.path import exists, join, isdir

    if exists(directory):
        all_data = [join(directory, x) for x in listdir(directory)]
        if recursively:
            for cc in all_data:
                if isdir(cc):
                    all_data.append(list_dir(cc, recursively, raise_not_exists))
        all_data.sort()
        return all_data
    
    elif not exists(directory) and raise_not_exists:
        raise FileNotFoundError(f"Directory not found: {directory}")
    else:
        logger.warning(f"Diretorio {directory} nao encontrado")
        return []

def __exctract(zip_file_name: str, target: str) -> None:
    """
    Descompacta um arquivo zip em uma pasta especificada

    :param zip_file_name: Caminho do arquivo zip
    :type zip_file_name: str

    :param target: Pasta destino (com ou sem o nome do arquivo)
    :type target: str

    """
    import zipfile

    logger.info(f"Extraindo dados de {zip_file_name} para {target}")
    with zipfile.ZipFile(zip_file_name, 'r') as z:
        z.extractall(target)
        z.close()

def __find_file(target_dir, reg_file_name, raise_error=False):
    """
    Baseado em um regex, busca um arquivo/diretorio em um diretorio fonrecido

    :param target_dir: Pasta alvo
    :type target_dir: str

    :param reg_file_name: Regex para localizar o arquivo
    :type reg_file_na,e: str

    :param raise_error: Se não encontrar nada, lançar um erro
    :type raise_error: bool, padrão False

    :return: Lista de arquivos encontrados
    :rtype: list[str]

    :raise FileNotFoundError: Se nenhum arquivo for encontrado e raise_error = True
    """
    
    logger.info(f"Buscando arquivo {reg_file_name} em {target_dir}")
    import re

    all_files = list_dir(target_dir)
    f = [x for x in all_files if re.match(reg_file_name, x)]
    logger.debug(all_files)
    logger.debug(f)

    if len(f) == 0 and raise_error:
        raise FileNotFoundError(f"There is no files called '{reg_file_name}'")
    
    logger.info(f"Arquivo(s) encontrado(s): {','.join(f)}")
    return f or []


def __download_files(url, download_dir: str, file_name: str, **kwargs):
    """
    Baixa arquivos uma determinada URL como em http://www.span.eggs/bacon.zip

    :param url: Url de destino
    :type url: str

    :param download_dir: Local para salvar o download
    :type download_dir: str

    :param file_name: Nome do arquivo a ser baixado
    :type file_name: str

    ### kwargs
    Aceita parâmetros para configurar o download
        retry: Define o número de tentativas de download em caso de falha, usa 3 por padrão
        retry_delay: Define o tempo de espera (em s) entre uma tentativa e outra, usa 30 por padrão
        timeout: Defime o tempo (em s) de espera máximo para um download, usa 50 por padrão
        stream_chunk_size: Define o tamanho das chunks no download, usa 8152 por padrão

    :raise ConnectionRefusedError: Se houve várias tentativas de conexão sem sucesso
    
    """
    from os import path, makedirs
    import requests


    ## Default args
    retry_times = kwargs.get("retry", 3)
    retry_delay = kwargs.get("retry_delay", 30)
    timemout = kwargs.get("timeout", 50)
    stream_chunk_size = kwargs.get("stream_chunk_size", 8152)

    logger.debug(f"Retry: {retry_times}")
    logger.debug(f"Delay: {retry_delay}")
    logger.debug(f"Timeout: {timemout}")
    
    # =====================================================

    # Cria diretorio de download se não existir
    __remove_directory(download_dir, recursively=True)  # remove se já existe e garante limpeza
    makedirs(download_dir, exist_ok=True)
    download_path = path.join(download_dir, file_name)


    # Loop de tentativas de download
    current_attempt = 0
    while current_attempt < retry_times:
        try:
            logger.info(f"Baixando dados de {url} [TENTATIVA {current_attempt + 1} de {retry_times}]")
            response = requests.get(url, stream=True, timeout=timemout)

            # Stream de dados
            if response.status_code != 200:
                msg = f"{response.status_code}: {response.text}"
                raise requests.exceptions.ConnectionError(msg)
            
            with open(download_path, 'wb') as f:
                logger.debug(f"Escrevendo em chunks de {stream_chunk_size}")
                for chunk in response.iter_content(stream_chunk_size):
                    f.write(chunk)
                f.close()
            
            logger.info("Pronto")
            logger.info(f"Output salvo em {download_path}")

            return download_path

        except Exception as ex:
            logger.error(str(ex))
            current_attempt += 1
            if current_attempt <= retry_times:
                # Se teve erro, remove tudo antes da proxima tentativa
                __remove_files(download_path)  
                logger.warning(f"Aguardando {retry_delay} segundos antes da proxima tentativa")
                sleep(retry_delay)
                logger.info(f"Retomando")
                continue
            else:
                raise ConnectionRefusedError(f"Max attempts exceded")


def read_pandas(file_name: str , **kwargs):
    """
    Le arquivos pandas em CSV

    :param file_name: Local do arquivo
    :type file_name: str

    ### kwargs
    Use os parâmetros padrão do pandas para leitura de CSV

    """
    logger.info(f"Lendo arquivo {file_name}")
    logger.debug(f"Confs: {kwargs}")
    return pd.read_csv(file_name, **kwargs)


def create_cep_table(
        df_logradouro: pd.DataFrame, 
        df_bairro: pd.DataFrame, 
        df_localidade: pd.DataFrame
    ) -> pd.DataFrame:
    import numpy as np

    """
    Baseado nos arquivos dos correios, gera uma base de CEP unica

    :param df_logradouro: Dataframe com todos os logradouros
    :type df_logradouro: pd.DataFrame

    :param df_bairro: DataFrame com todos os bairros
    :type df_bairro: pd.DataFrame

    :param df_localidade: DataFrame com todas as cidades
    :type df_localidade: pd.DataFrame

    :return: Base de CEP's consolidada
    :rtype: pd.DataFrame
    """

    def cast_as_string(df:pd.DataFrame):
        for i in df.columns:
            if i == "CEP":
                df[i] = df[i].astype(int)
                continue    
            df[i] = df[i].astype(str).replace('\n', '')
        return df

    # import numpy as np
    df_logradouro["LOGRADOURO"] = (df_logradouro["TLO_TX"].astype(str) + 
                                   " " + 
                                   df_logradouro["LOG_NO"].astype(str)
                                   ).str.strip()
    
    df_logradouro = df_logradouro.fillna(0).replace(np.inf, np.nan)
    df_bairro = df_bairro.fillna(0).replace(np.inf, np.nan)
    df_localidade = df_localidade.fillna(0).replace(np.inf, np.nan)
    
    m = {"M": "MUNICIPIO", "D": "DISTRITO", "P": "POVOADO"}
    df_localidade["DESC_LOCALIDADE"] = df_localidade["LOC_IN_TIPO_LOC"].map(m)

    # select cols   
    dict_log ={
        "CEP": "CEP",
        "LOGRADOURO": "LOGRADOURO",
        "LOG_COMPLEMENTO": "COMPLEMENTO_LOGRADOURO",
        "UFE_SG": "UF",
        "LOG_NU": "COD_LOGRADOURO",
        "BAI_NU_INI": "COD_BAIRRO",
        "LOC_NU": "COD_LOCALIDADE",        
    }

    dict_bai = {
        "BAI_NU": "COD_BAIRRO",
        "LOC_NU": "COD_LOCALIDADE",
        "BAI_NO": "NOME_BAIRRO",
    }

    dict_loc = {
        "LOC_NU": "COD_LOCALIDADE",
        "LOC_NO": "NOME_LOCALIDADE",
        "MUN_NU": "MUNICIPIO_IBGE",
        "DESC_LOCALIDADE": "DESC_LOCALIDADE"
    }

    df_logradouro = df_logradouro[list(dict_log.keys())]
    df_bairro = df_bairro[list(dict_bai.keys())]
    df_localidade = df_localidade[list(dict_loc.keys())]

    df_logradouro = df_logradouro.rename(columns=dict_log)
    df_bairro = df_bairro.rename(columns=dict_bai)
    df_localidade = df_localidade.rename(columns=dict_loc)

    df_logradouro.drop_duplicates()
    df_bairro.drop_duplicates()
    df_localidade.drop_duplicates()

    # string
    df_logradouro = cast_as_string(df_logradouro)
    df_bairro = cast_as_string(df_bairro)
    df_localidade = cast_as_string(df_localidade)

    df_logradouro = df_logradouro.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)
    df_bairro = df_bairro.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)
    df_localidade = df_localidade.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

    df_logradouro["COD_LOCALIDADE"] = pd.to_numeric(df_logradouro["COD_LOCALIDADE"], errors='coerce')
    df_logradouro["COD_BAIRRO"] = pd.to_numeric(df_logradouro["COD_BAIRRO"], errors='coerce')
    df_bairro["COD_BAIRRO"] = pd.to_numeric(df_bairro["COD_BAIRRO"], errors='coerce')
    df_localidade["COD_LOCALIDADE"] = pd.to_numeric(df_localidade["COD_LOCALIDADE"], errors='coerce')
    df_localidade["MUNICIPIO_IBGE"] = pd.to_numeric(df_localidade["MUNICIPIO_IBGE"], errors='coerce').astype(int)

    df_cep = df_logradouro.merge(
        df_localidade,
        on="COD_LOCALIDADE",
        how="left"
    )

    df_cep = df_cep.merge(
        df_bairro,
        on="COD_BAIRRO",
        how="left"
    )

    final_cols = [
        "CEP",
        "LOGRADOURO",
        "COMPLEMENTO_LOGRADOURO",
        "NOME_BAIRRO",
        "NOME_LOCALIDADE",
        "DESC_LOCALIDADE",
        "MUNICIPIO_IBGE",
        "UF"
    ]

    df_cep = df_cep[final_cols]
    df_cep = df_cep.replace(0, np.nan)
    df_cep = df_cep.dropna(how='all')
    

    return df_cep
    

def read_conf(conf_path="./conf/conf.yaml"):
    import yaml
    
    with open(conf_path, 'r') as f:
        data = yaml.safe_load(f)
        f.close()

    return data


def get_cep():
    """
    Orquestra o download de CEP da pagina dos correios

    As configurações abaixo podem ser usadas para definir local de download, arquivos usados
    e local de saida do XLSX
    """
    import re
    from os.path import dirname
    from os import makedirs

    # Define log level
    logger.setLevel(logging.INFO)

    conf = read_conf()
    sch_log = conf["columns"]["sch_log"]
    sch_bai = conf["columns"]["sch_bai"]
    sch_loc = conf["columns"]["sch_loc"]

    # Configuracoes inicias, opte por não mexer nelas
    url_correios = conf["correios"]["correios_url"]
    raw_zip_file = conf["correios"]["raw_zip_file"]
    re_zip_file = conf["correios"]["regex_conteudo"]
    target_files = conf["correios"]["regex_arquivos_validos"]
    csv_default_config = conf["correios"]["configuracoes_delimitado"]

    download_dir = conf["diretorios"]["download_dir"]
    output_file = conf["diretorios"]["saida"]["arquivo_de_saida"]
    output_cofig = conf["diretorios"]["saida"]["conf"]

    download_confs = conf.get("download", {})
    

    # ==============================================================================

    # download
    dwl_dir = __download_files(url_correios, download_dir, raw_zip_file, **download_confs)

    try:
        # Extai o arquivo principal na pasta de download
        __exctract(dwl_dir, download_dir)

        # Localiza e extrai o arquivo secundario
        out_file = __find_file(download_dir, re_zip_file, True)[0]
        __exctract(out_file, download_dir)

        # Localiza e lista os arquivos delimitados
        delimited_dir = __find_file(download_dir, ".*Delimitado", True)[0]
        content = __find_file(delimited_dir, target_files)

    except Exception as ex:
        sleep(2) # OS fechar qualquer arquivo ainda aberto
        __remove_directory(download_dir, True)  # Limpa tudo antes de sair
        logger.error(str(ex))
        exit(1)
    

    # Cria primeiros dataframes do pandas
    localidade = []
    bairro = []
    logradouro = []

    # Separa os arquivos segundo sua categoria
    for current_file in content:
        match = re.search(target_files, current_file)
        file_type = match.group(0)
        if file_type:
            if file_type.upper().__contains__("LOCALIDADE"):
                localidade.append(current_file)
            elif file_type.upper().__contains__("BAIRRO"):
                bairro.append(current_file)
            elif file_type.upper().__contains__("LOGRADOURO"):
                logradouro.append(current_file)

    # Le localidades e bairros
    df_localidade = read_pandas(localidade[0], **{**csv_default_config, **{"names": sch_loc}})
    df_bairro = read_pandas(bairro[0], **{**csv_default_config, **{"names": sch_bai}})
    
    # Le ruas/estradas/avenidas...
    df_logradouro = pd.DataFrame()
    for current_file in logradouro:
        temp = read_pandas(current_file, **{**csv_default_config, **{"names": sch_log}})
        df_logradouro = pd.concat([df_logradouro, temp])

    # Cria base de CEP consolidada
    df_cep = create_cep_table(df_logradouro, df_bairro, df_localidade)
    
    # Grava outputs
    makedirs(dirname(output_file), exist_ok=True)
    df_cep.to_csv(output_file, **output_cofig)
    logger.info(f"Base de CEPS salva em {output_file}")

    # Limpa tudo antes de sair
    # __remove_directory(download_dir, True)


if __name__ == "__main__":
    start = perf_counter()
    get_cep()
    end = perf_counter()
    logger.info(f"Duracao: {end - start} s")


