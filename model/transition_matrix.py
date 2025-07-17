from collections import defaultdict, Counter
from datetime import datetime
import numpy as np
from pprint import pprint

class Matrix_Transition:

    def analyze_transitions(self, data, output_format='markov_chain'):
        """
        Analiza las transiciones entre eventos y devuelve:
        - Un diccionario de probabilidades (Markov chain) o
        - Una matriz de transición con los estados

        Parámetros:
        data -- diccionario con los datos de eventos
        output_format -- 'markov_chain' (default) o 'transition_matrix'
        """
        # Validar datos de entrada
        if not data:
            return {} if output_format == 'markov_chain' else (None, None)

        # Extraer y ordenar eventos
        entries = []
        for v in data.values():
            try:
                event_time = datetime.fromisoformat(v["eventTime"].replace("Z", "+00:00"))
                event = v["event"]
                entries.append((event_time, event))
            except (KeyError, ValueError) as e:
                print(f"Advertencia: Entrada omitida - {e}")
                continue

        if not entries:
            return {} if output_format == 'markov_chain' else (None, None)

        entries.sort()  # Ordenar por tiempo

        # Crear transiciones (necesitamos al menos 2 eventos)
        if len(entries) < 2:
            return {} if output_format == 'markov_chain' else (None, None)

        transitions = []
        for i in range(len(entries) - 1):
            current = entries[i][1]
            next_event = entries[i + 1][1]
            transitions.append((current, next_event))

        # Identificar todos los estados únicos
        all_events = sorted({event for _, event in entries})

        if output_format == 'markov_chain':
            # Versión diccionario (Markov chain)
            transition_counts = defaultdict(Counter)
            for current, next_event in transitions:
                transition_counts[current][next_event] += 1

            markov_chain = {}
            for current, counter in transition_counts.items():
                total = sum(counter.values())
                markov_chain[current] = {k: v / total for k, v in counter.items()}

            return markov_chain

        elif output_format == 'transition_matrix':
            # Versión matriz de transición
            event_index = {event: idx for idx, event in enumerate(all_events)}
            n_states = len(all_events)

            # Inicializar y contar transiciones
            transition_counts = np.zeros((n_states, n_states), dtype=int)
            for current, next_event in transitions:
                current_idx = event_index[current]
                next_idx = event_index[next_event]
                transition_counts[current_idx][next_idx] += 1

            # Normalizar a probabilidades
            row_sums = transition_counts.sum(axis=1)
            transition_matrix = np.zeros_like(transition_counts, dtype=float)

            for i in range(n_states):
                if row_sums[i] > 0:
                    transition_matrix[i] = transition_counts[i] / row_sums[i]

            return all_events, transition_matrix

        else:
            raise ValueError("Formato de salida no válido. Use 'markov_chain' o 'transition_matrix'")