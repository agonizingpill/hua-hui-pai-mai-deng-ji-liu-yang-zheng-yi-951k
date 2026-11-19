export const ROLES={operator:'场内验级员',supervisor:'拍卖主管',viewer:'观察席'};
export const GRADES=['A','B','C'];
export const SAMPLE_STATES={recorded:'待估级',queued:'排队等候',running:'计算中',ready:'待主管核级',approved:'已核级',failed:'估级失败'};
export const LOT_STATES={offered:'挂场',sold:'已成交',cancelled:'已撤下'};
// 留样流水线展示状态：等候 / 补算 / 可核级，后端持久状态与代际、归档标志共同决定。
export function sampleStatus(x){
  let tone='wait',label=SAMPLE_STATES[x.state],hint='';
  if(x.state==='queued'||x.state==='running'){
    const retry=(x.compute_generation??1)>1;
    if(retry){tone='retry';label=x.state==='queued'?'补算排队':'补算中';hint=`中断后自动接续 · 第${x.compute_generation}代计算`}
    else{hint=x.state==='queued'?'已入队，等待worker领取':'worker正在计算，心跳保活中'}
  }else if(x.state==='ready'){
    if(x.artifact_ready){tone='ok';label='可以核级';hint='估值与归档齐备，且属于同一次计算'}
    else{tone='retry';label='归档核对中';hint='归档未齐或非本次计算，系统将自动补算'}
  }else if(x.state==='failed'){
    tone='retry';hint='可由验级员重新送估，或等待恢复器自动补送'
  }else if(x.state==='recorded'){
    hint='尚未送估'
  }else if(x.state==='approved'){
    tone='ok';hint='主管核定后不可再改'
  }
  return {tone,label,hint}
}
