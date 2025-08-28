from collections import defaultdict
import concurrent.futures

def contar_palabras(archivo):
    chunk_size = 4 * 1024 * 1024  # 4MB chunks
    max_workers = 10
    batch_size = max_workers * 2  # 20
    
    def procesar_chunk(chunk_data):
        contador_local = 0
        indice_local = defaultdict(int)
        
        palabras = chunk_data.split()
        contador_local += len(palabras)
        
        for palabra in palabras:
            palabra_limpia = palabra.lower().strip('.,!?";:()[]{}')
            if palabra_limpia:
                indice_local[palabra_limpia] += 1
        
        return contador_local, dict(indice_local)
    
    contador_total = 0
    indice_total = defaultdict(int)
    buffer_anterior = ""
    
    with open(archivo, 'r', encoding='utf-8') as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    if buffer_anterior:
                        future = executor.submit(procesar_chunk, buffer_anterior)
                        futures.append(future)
                    break
                
                texto_completo = buffer_anterior + chunk
                palabras = texto_completo.split()
                
                if len(palabras) > 1:
                    chunk_procesable = ' '.join(palabras[:-1])
                    future = executor.submit(procesar_chunk, chunk_procesable)
                    futures.append(future)
                    buffer_anterior = palabras[-1]
                else:
                    buffer_anterior = texto_completo
                
                if len(futures) >= batch_size:
                    for future in concurrent.futures.as_completed(futures[:max_workers]):
                        contador, indice = future.result()
                        contador_total += contador
                        for palabra, freq in indice.items():
                            indice_total[palabra] += freq
                    futures = futures[max_workers:]
            
            for future in concurrent.futures.as_completed(futures):
                contador, indice = future.result()
                contador_total += contador
                for palabra, freq in indice.items():
                    indice_total[palabra] += freq
    
    return contador_total, dict(indice_total)

if __name__ == "__main__":
    archivo = r"D:\Codigos\TopicosPrimeraTarea\wikipedia.txt"
    
    print("Procesando archivo...")
    total_palabras, indice = contar_palabras(archivo)
    
    print(f"El archivo tiene {total_palabras} palabras.")
    print(f"Palabras únicas: {len(indice)}")
    
    palabras_frecuentes = sorted(indice.items(), key=lambda x: x[1], reverse=True)
    print("\nTodas las palabras y su frecuencia:")
    for palabra, frecuencia in palabras_frecuentes:
        print(f"{palabra}: {frecuencia}")