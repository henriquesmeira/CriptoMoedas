import subprocess
import sys
import time

def run_script(script_name, wait_after=False):
    """
    Executa um script Python.
    
    Args:
        script_name: Nome do arquivo do script a ser executado
        wait_after: Se True, espera 30 segundos após a execução
    """
    try:
        print(f"Executando {script_name}...")
        subprocess.run([sys.executable, script_name], check=True)
        print(f"{script_name} executado com sucesso.")
        
        if wait_after:
            print(f"Aguardando 30 segundos antes da próxima requisição...")
            time.sleep(30)
            
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script_name}: {e}")
        return False
    
    return True

def main():
    # Scripts da API que precisam de intervalo entre execuções
    api_scripts = [
        "cadastro.py",
        "candle.py",
        "historicocriptos.py",
        "volume.py"
    ]
    
    # Executa os scripts da API com espera de 30 segundos entre eles
    all_succeeded = True
    for i, script in enumerate(api_scripts):
        # Verifica se é o último script da API para não esperar desnecessariamente
        wait_needed = i < len(api_scripts) - 1
        success = run_script(script, wait_after=wait_needed)
        if not success:
            all_succeeded = False
            print(f"Falha na execução do script {script}.")
    
    # Executa o database_manager apenas se todos os scripts anteriores foram bem-sucedidos
    if all_succeeded:
        print("Todos os scripts da API foram executados. Executando database_manager.py...")
        run_script("database_manager.py")
    else:
        print("Alguns scripts falharam. Database manager não será executado.")

if __name__ == "__main__":
    main()
