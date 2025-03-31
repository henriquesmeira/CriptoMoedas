import subprocess
import sys
import time
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constantes
TEMPO_ESPERA_API = 30  # segundos para esperar entre chamadas à API


def run_script(script_name, wait_after=False):
    """
    Executa um script Python com tratamento adequado de erros.
    
    Args:
        script_name: Nome do arquivo de script a ser executado
        wait_after: Se True, espera TEMPO_ESPERA_API segundos após a execução
        
    Returns:
        Booleano indicando sucesso ou falha
    """
    try:
        logger.info(f"Executando {script_name}...")
        subprocess.run([sys.executable, script_name], check=True)
        logger.info(f"{script_name} executado com sucesso.")
        
        if wait_after:
            logger.info(f"Aguardando {TEMPO_ESPERA_API} segundos antes da próxima requisição...")
            time.sleep(TEMPO_ESPERA_API)
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar {script_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado ao executar {script_name}: {e}")
        return False
    
    return True


def main():
    """
    Função principal para orquestrar a execução de todos os scripts.
    """
    logger.info("Iniciando sequência de execução de scripts")
    
    # Scripts da API que precisam de intervalos entre execuções
    api_scripts = [
        "cadastro.py",
        "historicocriptos.py",
        "volume.py"
    ]
    
    # Executa scripts da API com tempo de espera entre eles
    all_succeeded = True
    for i, script in enumerate(api_scripts):
        # Verifica se é o último script da API para evitar espera desnecessária
        wait_needed = i < len(api_scripts) - 1
        success = run_script(script, wait_after=wait_needed)
        
        if not success:
            all_succeeded = False
            logger.warning(f"Falha ao executar o script {script}.")
    
    # Executa o database_manager apenas se todos os scripts anteriores foram bem-sucedidos
    if all_succeeded:
        logger.info("Todos os scripts da API foram executados com sucesso. Executando database_manager.py...")
        db_success = run_script("database_manager.py")
        
        if db_success:
            logger.info("Pipeline de dados completo executado com sucesso.")
        else:
            logger.error("Execução do gerenciador de banco de dados falhou.")
    else:
        logger.error("Alguns scripts falharam. O gerenciador de banco de dados não será executado.")


if __name__ == "__main__":
    main()