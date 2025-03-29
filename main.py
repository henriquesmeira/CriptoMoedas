import subprocess
import sys

def run_script(script_name):
    """Executa um script Python."""
    try:
        print(f"Executando {script_name}...")
        subprocess.run([sys.executable, script_name], check=True)
        print(f"{script_name} executado com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {script_name}: {e}")

def main():
    # Rodar os scripts na ordem necessária
    scripts = [
        "cadastro.py",
        "candle.py",
        "historicocriptos.py",
        "volume.py",
        "database_manager.py"
    ]
    
    for script in scripts:
        run_script(script)

if __name__ == "__main__":
    main()
