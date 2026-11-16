import { computed, ref, shallowRef, triggerRef } from "vue";
import type { TrafficMessage, Which } from "@/api/types";

const capacity = 30;

/** the last seconds the busiest member is voted over; a one-second burst
 * (an observatory probe download) cannot outvote sustained traffic */
const voteWindow = 10;

/** one member outbound that carried traffic since the last one-second sample */
export interface ActiveOutbound {
  which: Which;
  up: number;
  down: number;
}

/** createTraffic retains the last thirty one-second traffic frames, oldest sample first. */
export function createTraffic() {
  const up = ref(0);
  const down = ref(0);
  const upTotal = ref(0);
  const downTotal = ref(0);
  const upSamples = shallowRef<number[]>([]);
  const downSamples = shallowRef<number[]>([]);
  const active = shallowRef<ActiveOutbound[]>([]);
  const activeHistory = shallowRef<ActiveOutbound[][]>([]);
  let next = 0;

  // oldest first, padded with zeros to the window so the chart is a flat
  // line before the first frame and keeps one time scale as frames come in
  const ordered = (samples: number[]) => [
    ...new Array<number>(capacity - samples.length).fill(0),
    ...samples.map((_, i) => samples[(next + i) % samples.length]),
  ];
  const upSeries = computed(() => ordered(upSamples.value));
  const downSeries = computed(() => ordered(downSamples.value));

  /** the member that carried traffic in the most of the last seconds,
   * tie-broken by bytes; undefined while the network is idle */
  const busiest = computed<ActiveOutbound | undefined>(() => {
    type Stat = { count: number; up: number; down: number; which: Which };
    const seconds = new Map<string, Stat>();
    for (const frame of activeHistory.value) {
      for (const entry of frame) {
        if (entry.up + entry.down <= 0) continue;
        const key = `${entry.which._type}/${entry.which.id}/${entry.which.sub ?? ""}`;
        let stat = seconds.get(key);
        if (!stat) {
          stat = { count: 0, up: 0, down: 0, which: entry.which };
          seconds.set(key, stat);
        }
        stat.count += 1;
        stat.up += entry.up;
        stat.down += entry.down;
      }
    }
    let best: Stat | undefined;
    for (const stat of seconds.values()) {
      if (
        !best ||
        stat.count > best.count ||
        (stat.count === best.count && stat.up + stat.down > best.up + best.down)
      ) {
        best = stat;
      }
    }
    return best ? { which: best.which, up: best.up, down: best.down } : undefined;
  });

  function feed({ body }: TrafficMessage) {
    up.value = body.up;
    down.value = body.down;
    upTotal.value = body.upTotal;
    downTotal.value = body.downTotal;
    active.value = body.active ?? [];
    activeHistory.value = [...activeHistory.value, body.active ?? []].slice(
      -voteWindow,
    );
    upSamples.value[next] = body.up;
    downSamples.value[next] = body.down;
    next = (next + 1) % capacity;
    triggerRef(upSamples);
    triggerRef(downSamples);
    triggerRef(active);
  }

  function reset() {
    next = 0;
    up.value = down.value = upTotal.value = downTotal.value = 0;
    upSamples.value = [];
    downSamples.value = [];
    active.value = [];
    activeHistory.value = [];
  }

  return { feed, reset, up, down, upTotal, downTotal, upSeries, downSeries, active, busiest };
}

// One stream for the page: the shell feeds it from the message socket
// and resets it with the session; the dashboard reads it.
const shared = createTraffic();

export function useTraffic() {
  return shared;
}
