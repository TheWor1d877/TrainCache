/* 误差棒与哑铃连线：把 option 里的 __errorbars / __dumbbells 渲染成图元。
   Node 端与浏览器端共用同一份实现，option 里只放数据，不放函数。 */
(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  } else {
    root.addErrorBars = api.addErrorBars;
    root.addDumbbells = api.addDumbbells;
  }
})(this, function () {
  function addErrorBars(option) {
    const specs = option.__errorbars || [];
    delete option.__errorbars;
    specs.forEach(function (spec) {
      if (!spec.points || !spec.points.length) return;
      option.series.push({
        type: "custom",
        silent: true,
        z: 1,
        xAxisIndex: spec.xAxisIndex,
        yAxisIndex: spec.yAxisIndex,
        data: spec.points,
        renderItem: function (params, api) {
          const xi = api.value(0);
          const low = api.coord([xi, api.value(1)]);
          const high = api.coord([xi, api.value(2)]);
          const cap = 4;
          const style = { stroke: spec.color, lineWidth: 1, opacity: 0.55 };
          return {
            type: "group",
            children: [
              { type: "line", shape: { x1: low[0], y1: low[1], x2: high[0], y2: high[1] }, style: style },
              { type: "line", shape: { x1: low[0] - cap, y1: low[1], x2: low[0] + cap, y2: low[1] }, style: style },
              { type: "line", shape: { x1: low[0] - cap, y1: high[1], x2: low[0] + cap, y2: high[1] }, style: style },
            ],
          };
        },
      });
    });
    return option;
  }

  function addDumbbells(option) {
    const specs = option.__dumbbells || [];
    delete option.__dumbbells;
    specs.forEach(function (spec) {
      if (!spec.points || !spec.points.length) return;
      option.series.push({
        type: "custom",
        silent: true,
        z: 1,
        xAxisIndex: spec.xAxisIndex,
        yAxisIndex: spec.yAxisIndex,
        data: spec.points,
        renderItem: function (params, api) {
          const yi = api.value(2);
          const a = api.coord([api.value(0), yi]);
          const b = api.coord([api.value(1), yi]);
          return {
            type: "line",
            shape: { x1: a[0], y1: a[1], x2: b[0], y2: b[1] },
            style: { stroke: spec.color, lineWidth: 3, opacity: 0.28 },
          };
        },
      });
    });
    return option;
  }

  return { addErrorBars: addErrorBars, addDumbbells: addDumbbells };
});