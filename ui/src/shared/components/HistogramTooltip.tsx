import React, {SFC} from 'react'
import {HistogramTooltipProps} from 'src/minard'
import {format} from 'd3-format'

import 'src/shared/components/HistogramTooltip.scss'

const formatLarge = format('.4~s')
const formatSmall = format('.4~g')
const formatBin = n => (n < 1 && n > -1 ? formatSmall(n) : formatLarge(n))

const HistogramTooltip: SFC<HistogramTooltipProps> = ({
  fillColumnName,
  binStart,
  binStop,
  counts,
}) => {
  return (
    <div className="histogram-tooltip">
      <div className="histogram-tooltip--bin">
        {formatBin(binStart)}
        &ndash;
        {formatBin(binStop)}
      </div>
      <div className="histogram-tooltip--table">
        <div className="histogram-tooltip--fills">
          <div className="histogram-tooltip--column-header">
            {fillColumnName}
          </div>
          {counts.map(({fill, color}) => (
            <div key={color} style={{color}}>
              {fill}
            </div>
          ))}
        </div>
        <div className="histogram-tooltip--counts">
          <div className="histogram-tooltip--column-header">count</div>
          {counts.map(({count, color}) => (
            <div key={color} style={{color}}>
              {count}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

export default HistogramTooltip
