import React, {useState, useEffect, SFC} from 'react'
import uuid from 'uuid'

import {PlotEnv} from 'src/minard'
import * as stats from 'src/minard/utils/stats'
import {assert} from 'src/minard/utils/assert'
import {registerLayer, unregisterLayer} from 'src/minard/utils/plotEnvActions'
import HistogramBars from 'src/minard/components/HistogramBars'
import HistogramTooltip from 'src/minard/components/HistogramTooltip'

export enum Position {
  Stacked = 'stacked',
  Overlaid = 'overlaid',
}

export interface Props {
  env: PlotEnv
  x?: string
  fill?: string
  position?: Position
  bins?: number
  colors?: string[]
  tooltip?: (props: TooltipProps) => JSX.Element
}

export interface TooltipProps {
  xColumnName: string
  fillColumnName: string
  binStart: number
  binStop: number
  counts: Array<{fill: string | number | boolean; count: number; color: string}>
}

export const Histogram: SFC<Props> = props => {
  const [layerKey] = useState(() => uuid.v4())

  const {bins, position} = props
  const {layers, defaults, dispatch} = props.env
  const layer = layers[layerKey]
  const table = defaults.table
  const x = props.x || defaults.aesthetics.x
  const fill = props.fill || defaults.aesthetics.fill
  const colors = props.colors

  useEffect(
    () => {
      const xCol = table.columns[x]
      const xColType = table.columnTypes[x]
      const fillCol = table.columns[fill]
      const fillColType = table.columnTypes[fill]

      assert('expected an `x` aesthetic', !!x)
      assert(`table does not contain column "${x}"`, !!xCol)

      const [statTable, mappings] = stats.bin(
        xCol,
        xColType,
        fillCol,
        fillColType,
        bins,
        position
      )

      dispatch(registerLayer(layerKey, statTable, mappings, colors))

      return () => dispatch(unregisterLayer(layerKey))
    },
    [table, x, fill, position, bins, colors]
  )

  if (!layer) {
    return null
  }

  const {
    innerWidth,
    innerHeight,
    defaults: {
      scales: {x: xScale, y: yScale, fill: layerFillScale},
    },
  } = props.env

  const {
    aesthetics,
    table: {columns},
    scales: {fill: defaultFillScale},
  } = layer

  const fillScale = layerFillScale || defaultFillScale
  const {mouseX, mouseY} = props.env

  return (
    <>
      <HistogramBars
        width={innerWidth}
        height={innerHeight}
        xMin={columns[aesthetics.xMin]}
        xMax={columns[aesthetics.xMax]}
        yMin={columns[aesthetics.yMin]}
        yMax={columns[aesthetics.yMax]}
        fill={columns[aesthetics.fill]}
        fillScale={fillScale}
        xScale={xScale}
        yScale={yScale}
        position={props.position || Position.Stacked}
      />
      {mouseX &&
        mouseY && (
          <HistogramTooltip
            mouseX={mouseX}
            mouseY={mouseY}
            width={innerWidth}
            height={innerHeight}
            xMin={columns[aesthetics.xMin]}
            xMax={columns[aesthetics.xMax]}
            yMin={columns[aesthetics.yMin]}
            yMax={columns[aesthetics.yMax]}
            fill={columns[aesthetics.fill]}
            fillScale={fillScale}
            xScale={xScale}
            yScale={yScale}
            xColumnName={x}
            fillColumnName={fill}
            body={props.tooltip}
          />
        )}
    </>
  )
}
