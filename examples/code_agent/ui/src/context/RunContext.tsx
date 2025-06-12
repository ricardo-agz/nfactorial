import React from 'react';
import type { Run, Action } from '../types/run';

interface RunContextValue {
  runs: Record<string, Run>;
  runOrder: string[];
  createRun: (runId: string, userPrompt: string) => void;
  addAction: (runId: string, action: Action) => void;
  updateAction: (runId: string, actionId: string, updater: (prev: Action | undefined) => Action) => void;
}

const RunContext = React.createContext<RunContextValue | undefined>(undefined);

interface RunState {
  runs: Record<string, Run>;
  order: string[];
}

function runReducer(state: RunState, action: { type: string; payload: unknown }): RunState {
  switch (action.type) {
    case 'create': {
      const { id, prompt } = action.payload as { id: string; prompt: string };
      if (state.runs[id]) return state;
      return {
        runs: {
          ...state.runs,
          [id]: { id, userPrompt: prompt, actions: [] },
        },
        order: [...state.order, id],
      };
    }
    case 'addAction': {
      const { runId, newAction } = action.payload as { runId: string; newAction: Action };
      const existing = state.runs[runId];
      if (!existing) return state;
      return {
        runs: {
          ...state.runs,
          [runId]: { ...existing, actions: [...existing.actions, newAction] },
        },
        order: state.order,
      };
    }
    case 'updateAction': {
      const { runId, actionId, updater } = action.payload as {
        runId: string;
        actionId: string;
        updater: (prev: Action | undefined) => Action;
      };
      const existingRun = state.runs[runId];
      if (!existingRun) return state;
      const idx = existingRun.actions.findIndex(a => a.id === actionId);
      let updatedActions: Action[];
      if (idx === -1) {
        updatedActions = [...existingRun.actions, updater(undefined)];
      } else {
        const updated = updater(existingRun.actions[idx]);
        updatedActions = [...existingRun.actions];
        updatedActions[idx] = updated;
      }
      return {
        runs: { ...state.runs, [runId]: { ...existingRun, actions: updatedActions } },
        order: state.order,
      };
    }
    default:
      return state;
  }
}

export const RunProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [state, dispatch] = React.useReducer(runReducer, { runs: {}, order: [] } as RunState);

  const createRun = React.useCallback((runId: string, userPrompt: string) => {
    dispatch({ type: 'create', payload: { id: runId, prompt: userPrompt } });
  }, []);

  const addAction = React.useCallback((runId: string, action: Action) => {
    dispatch({ type: 'addAction', payload: { runId, newAction: action } });
  }, []);

  const updateAction = React.useCallback(
    (
      runId: string,
      actionId: string,
      updater: (prev: Action | undefined) => Action,
    ) => {
      dispatch({ type: 'updateAction', payload: { runId, actionId, updater } });
    },
    [],
  );

  const value = React.useMemo<RunContextValue>(() => ({ runs: state.runs, runOrder: state.order, createRun, addAction, updateAction }), [state, createRun, addAction, updateAction]);

  return <RunContext.Provider value={value}>{children}</RunContext.Provider>;
};

export const useRuns = (): RunContextValue => {
  const ctx = React.useContext(RunContext);
  if (!ctx) throw new Error('useRuns must be used within RunProvider');
  return ctx;
}; 