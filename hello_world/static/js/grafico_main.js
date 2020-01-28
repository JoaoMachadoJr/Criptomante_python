var dom = document.getElementById("container");
            var myChart = echarts.init(dom);
            var app = {};
            option = null;
            option = {
                title: {
                    text: 'Indicador de valor do Bitcoin',
                    subtext: 'Consolidação de todos os algoritmos'
                },
                tooltip: {
                    trigger: 'axis'
                },
                legend: {
                    data:['Legenda 1','Legenda 2']
                },
                toolbox: {
                    show: true,
                    feature: {
                        dataZoom: {
                            yAxisIndex: 'none'
                        },
                        dataView: {readOnly: true},
                        magicType: {type: ['line', 'bar']},
                        restore: {},
                        saveAsImage: {}
                    }
                },
                xAxis:  {
                    type: 'category',
                    boundaryGap: false,
                    data: [<?php echo nprevisao::labels($previsoes)  ?>]
                },
                yAxis: {
                    type: 'value',
                    axisLabel: {
                        formatter: '{value}'
                    },
                    min: <?php echo nprevisao::limite_inferior($previsoes)  ?>
                },
                series: [
                    {
                        name:'serie1',
                        type:'line',
                        data:[<?php echo nprevisao::valores($previsoes)  ?>],
                        /*markPoint: {
                            data: [
                                {type: 'max', name: 'nome1.1'},
                                {type: 'min', name: 'nome1.2'}
                            ]
                        }*/  /*,
                        markLine: {
                            data: [
                                {type: 'average', name: 'media'}
                            ]
                        }*/
                    }/*,
                    {
                        name:'serie2',
                        type:'line',
                        data:[1, -2, 2, 5, 3, 2, 0],
                        markPoint: {
                            data: [
                                {name: 'nome2.1', value: -2, xAxis: 1, yAxis: -1.5}
                            ]
                        },
                        markLine: {
                            data: [
                                {type: 'average', name: 'media2'},
                                [{
                                    symbol: 'none',
                                    x: '90%',
                                    yAxis: 'max'
                                }, {
                                    symbol: 'circle',
                                    label: {
                                        normal: {
                                            position: 'start',
                                            formatter: 'label1'
                                        }
                                    },
                                    type: 'max',
                                    //name: 'nome3'
                                }]
                            ]
                        }
                    }*/
                ]
            };
            ;
            if (option && typeof option === "object") {
                myChart.setOption(option, true);
            }