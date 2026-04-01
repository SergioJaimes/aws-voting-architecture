# AWS Voting Architecture

Implementación de la táctica de disponibilidad **Voting (Triple Modular Redundancy)** en AWS.

## Componentes

- Lambda Orchestrator
- EventBridge
- Lambda Calc A/B/C
- DynamoDB
- Lambda Voter
- DynamoDB Streams

## Flujo

1. Orchestrator recibe request
2. Publica 3 eventos (A, B, C)
3. Lambdas calculan en paralelo
4. Guardan resultados en DynamoDB
5. Stream activa Voter
6. Voter aplica majority voting
7. Resultado final se persiste

## Ejemplo Voting (2 de 3)

- A = 490000  
- B = 490000  
- C = 450000  

Resultado:
- voteStatus = 2_OF_3  
- divergentReplica = C  

## Táctica implementada

Voting (detección de fallas)