import ButtonSend from '../Button/ButtonSend.tsx';
import ButtonBack from '../Button/ButtonBack.tsx';
import ResultTable from '../Table/ResultTable.tsx';
import Store from '../../store/Store.ts';
import { useEffect, useState } from 'react';
import { observer } from 'mobx-react-lite';

const Result = observer(() => {
    const [resultsWeight, setResultsWeight] = useState<{ [key: string]: string }>({});
    const [resultsQuantity, setResultsQuantity] = useState<{ [key: string]: string }>({});

    useEffect(() => {
        setResultsWeight({
            ...Object.fromEntries(
                Store.resultWeight[0].Stocks.map((item) => [item.name, (parseFloat(item.value) * 100).toFixed(2).toString() + '%']),
            ),
            'Риск портфеля': (parseFloat(Store.resultWeight[0].Portfolio_Risk.toString()) * 100).toFixed(2).toString() + '%',
            'Доходность портфеля': (parseFloat(Store.resultWeight[0].Portfolio_Return.toString()) * 100).toFixed(2).toString() + '%',
        });
        if (Store.resultQuantity.length !== 0 && !Store.isClientOptimization) {
            setResultsQuantity({
                ...Object.fromEntries(
                    Store.resultQuantity[0].Stocks.map((item) => [item.name, parseInt(item.value).toString() + ' ' + 'шт.']),
                ),
            });
        }
    }, [Store.resultWeight]);
    return (
        <div>
            <h1
                style={{
                    fontSize: '64px',
                    marginBottom: '32px',
                }}>
                {Store.isClientOptimization ? 'Результат' : 'Результат оптимизации'}
            </h1>
            <div
                style={{
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'center',
                    alignItems: 'center',
                    gap: '32px',
                    marginBottom: '32px',
                }}>
                {!Store.isClientOptimization && (
                    <h2>
                        Вариант оптимизации:{' '}
                        <span
                            style={{
                                fontWeight: 'bold',
                                color: 'green',
                                borderBottom: '1px solid green',
                            }}>
                            {typeof Store.resultWeight[0] !== 'undefined'
                                ? Store.resultWeight[0].Optimization.toString().toLowerCase()
                                : ''}
                        </span>
                    </h2>
                )}

                <div>
                    <h3 style={{ marginBottom: '4px' }}>Веса активов</h3>
                    <ResultTable results={resultsWeight} />
                </div>
                {!Store.isClientOptimization && (
                    <div>
                        <h3 style={{ marginBottom: '4px' }}>Количество активов</h3>
                        <ResultTable results={resultsQuantity} />
                    </div>
                )}
            </div>
            <div
                style={{
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'center',
                    alignItems: 'center',
                    gap: '32px',
                    marginBottom: '48px',
                }}>
                <h2 style={{ fontSize: '32px' }}>Историческая и ожидаемая доходность собранного портфеля</h2>
                <img style={{ textAlign: 'center' }} src={`data:image/png; base64, ${Store.getImageFigure}`} alt={'result'}></img>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-evenly', marginBottom: '24px' }}>
                <ButtonBack
                    title={'Выбрать другой портфель'}
                    onClick={() => {
                        if (Store.currentOptimizationType === 'free') {
                            Store.setCurrentPage = 'freeOptimization';
                        } else {
                            Store.setCurrentPage = 'paidOptimization';
                        }
                        Store.setResultWeight = [];
                        Store.setResultQuantity = [];
                        Store.setIsClientOptimization = false;
                    }}
                />
                <ButtonSend
                    title={'Ввести свои активы'}
                    style={{ backgroundColor: 'purple' }}
                    onClick={() => {
                        Store.setCurrentPage = 'clientOptimization';
                        Store.setResultWeight = [];
                        Store.setResultQuantity = [];
                        Store.setCurrentOptimizationType = 'free';
                        Store.setIsClientOptimization = true;
                    }}
                />
                <ButtonSend
                    title={Store.currentOptimizationType === 'free' ? 'Перейти к платной оптимизации' : 'Перейти к бесплатной оптимизации'}
                    onClick={() => {
                        if (Store.currentOptimizationType === 'free') {
                            Store.setCurrentPage = 'paidOptimization';
                            Store.setCurrentOptimizationType = 'paid';
                        } else {
                            Store.setCurrentPage = 'freeOptimization';
                            Store.setCurrentOptimizationType = 'free';
                        }
                        Store.setResultWeight = [];
                        Store.setResultQuantity = [];
                        Store.setIsClientOptimization = false;
                    }}
                />
            </div>
            <div style={{ display: 'flex', justifyContent: 'center' }}>
                <ButtonBack
                    style={{ width: '100%', backgroundColor: 'green' }}
                    title={'Вернуться на главную'}
                    onClick={() => {
                        Store.currentPage = 'greeting';
                        Store.setResultWeight = [];
                        Store.setResultQuantity = [];
                        Store.setCurrentOptimizationType = 'free';
                        Store.setCurrencys = [];
                        Store.setCurrency = '';
                        Store.setIsClientOptimization = false;
                    }}
                />
            </div>
        </div>
    );
});

export default Result;
