
from mainNeo4j import Neo4jCRUD
uri = "bolt://localhost:7687"  
user = "neo4j"
password = "password"
neo4j = Neo4jCRUD(uri, user, password)
        
while True:
    print("\nMenu:")
    print("1. Obtener todas las atracciones visitadas por un visitante específico")
    print("2. Mostrar las 5 atracciones más visitadas.")
    print("3. Coches alquilados por un cliente específico")
    print("4. Muestra los equipos con el número total de proyectos a los que están asociados")
    print("5. Mostrar todos los coches que fueron alquilados más de una vez")
    print("6. Exit")
    choice = input("Enter your choice (1/2/3/4/6): ")
    
    if choice == '1':
        visitante = input("Introduce el nombre del visitante ")
        atracciones=neo4j.atracciones_visitante(visitante)
        for element in atracciones:
            print(element)
    elif choice == '2':
        print("a")
    elif choice == '3':
        print("a")
    elif choice == '4':
        print("a")
    elif choice == '5':
        print("a")
    elif choice == '6':
        print("a")
    else:
        print("Invalid choice. Please select a valid option.")
