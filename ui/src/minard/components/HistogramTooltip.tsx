import React, {useRef, SFC} from 'react'
import {range} from 'd3-array'

import {Scale, HistogramTooltipProps} from 'src/minard'
import {useLayoutStyle} from 'src/minard/utils/useLayoutStyle'

const MARGIN_X = 15
const MARGIN_Y = 10

interface Props {
  mouseX: number
  mouseY: number
  xColumnName: string
  fillColumnName: string
  body?: (props: HistogramTooltipProps) => JSX.Element
  width: number
  height: number
  xMin: number[]
  xMax: number[]
  yMin: number[]
  yMax: number[]
  fill: string[] | boolean[]
  xScale: Scale<number, number>
  yScale: Scale<number, number>
  fillScale: Scale<string | number | boolean, string>
}

const HistogramTooltip: SFC<Props> = ({
  mouseX,
  mouseY,
  xColumnName,
  fillColumnName,
  body,
  width,
  height,
  xMin,
  xMax,
  yMin,
  yMax,
  fill,
  xScale,
  yScale,
  fillScale,
}: Props) => {
  const tooltip = useRef<HTMLDivElement>(null)

  useLayoutStyle(tooltip, ({offsetWidth, offsetHeight}) => {
    const transX =
      mouseX + MARGIN_X + offsetWidth > width
        ? 0 - MARGIN_X - offsetWidth
        : MARGIN_X

    const transY =
      mouseY + MARGIN_Y + offsetHeight > height
        ? 0 - MARGIN_Y - offsetHeight
        : MARGIN_Y

    return {
      position: 'absolute',
      left: `${mouseX + transX}px`,
      top: `${mouseY + transY}px`,
    }
  })

  const dataX = xScale.invert(mouseX)
  const dataY = yScale.invert(mouseY)

  const rowIndices = range(0, xMin.length).filter(
    i => xMin[i] <= dataX && xMax[i] > dataX
  )

  if (!rowIndices.length) {
    return null
  }

  if (!rowIndices.some(i => yMax[i] >= dataY)) {
    return null
  }

  const tooltipProps: HistogramTooltipProps = {
    xColumnName,
    fillColumnName,
    binStart: xMin[rowIndices[0]],
    binStop: xMax[rowIndices[0]],
    counts: rowIndices.map(i => ({
      fill: fill[i],
      count: yMax[i] - yMin[i],
      color: fillScale(fill[i]),
    })),
  }

  // TODO: Default tooltip implementation
  return (
    <div className="minard-histogram-tooltip" ref={tooltip}>
      {body ? body(tooltipProps) : null}
    </div>
  )
}

export default HistogramTooltip
