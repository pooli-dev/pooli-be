import { buildK6Scenario } from './k6_scaled_traffic_lib.js';

// 1000-line scale test entrypoint.
// - lineId: 1~1000
// - familyId: 1~250 (4 lines per family)
// - request interval: 1 second
// - per-line attempted usage: 50MB (1~3MB chunks)
const scenario = buildK6Scenario(1000);

export const options = {
  ...scenario.options,
  scenarios: {
    small_traffic_1000: {
      ...scenario.options.scenarios.scaled_traffic,
    },
  },
};

export const setup = scenario.setup;
export default scenario.default;
