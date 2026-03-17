import { buildK6Scenario } from './k6_scaled_traffic_lib.js';

// 100-line scale test entrypoint.
// - lineId: 1~100
// - familyId: 1~25 (4 lines per family)
// - request interval: 1 second
// - per-line attempted usage: 50MB (1~3MB chunks)
const scenario = buildK6Scenario(100);

export const options = {
  ...scenario.options,
  scenarios: {
    small_traffic_100: {
      ...scenario.options.scenarios.scaled_traffic,
    },
  },
};

export const setup = scenario.setup;
export default scenario.default;
