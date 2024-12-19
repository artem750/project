import ButtonSend from '../Button/ButtonSend.tsx';
import Store from '../../store/Store.ts';
import MultipleSelect from '../Select/MultipleSelect.tsx';
import SelectOne from '../Select/SelectOne.tsx';
import * as React from 'react';
import axios from 'axios';
import { observer } from 'mobx-react-lite';
import ButtonBack from '../Button/ButtonBack.tsx';

const optimizationTypeFree = ['Максимизация доходности', 'Минимизация риска', 'Максимизация коэффициента Шарпа'];
const optimizationTypePaid = ['Агрессивный', 'Умеренно-агрессивный', 'Рациональный', 'Умеренно-консервативный', 'Консервативный'];
type OptimizationType = {
    type: 'free' | 'paid';
};

const Optimization: React.FC<OptimizationType> = observer(({ type }) => {
    const [typeOptimizationValue, setTypeOptimizationValue] = React.useState<string>('');
    const [actives, setActives] = React.useState<string[]>([]);

    function parseResults(data: [{ [key: string]: string }]) {
        return data.map((item) => {
            const stocks = Object.keys(item)
                .filter((key) => key !== 'Optimization' && key !== 'Portfolio_Risk' && key !== 'Portfolio_Return')
                .map((key) => ({ name: key, value: item[key] }));
            return {
                Optimization: item.Optimization,
                Portfolio_Return: item.Portfolio_Return,
                Portfolio_Risk: item.Portfolio_Risk,
                Stocks: stocks,
            };
        });
    }

    function makeFreeOptimization(activ: string[], typeOptimization: string) {
        if (activ.length !== 0 && typeOptimization !== '') {
            if (activ.length < 2) {
                alert('Минимальное количество активов - 2');
                return;
            } else {
                axios
                    .post(type === 'free' ? 'http://127.0.0.1:8000/optimize/else' : 'http://127.0.0.1:8000/optimize/profile', {
                        securities: activ,
                        target: typeOptimization,
                    })
                    .then((response) => {
                        Store.setResultWeight = parseResults(response.data.data);
                        Store.setResultQuantity = parseResults(response.data.quantity_df);
                        Store.setImageFigure = response.data.image;
                        Store.setCurrentPage = 'result';
                    })
                    .catch((error) => console.error(error));
            }
        }
    }

    return (
        <div>
            <h1 style={{ fontSize: '64px', marginBottom: '32px' }}>Выбор инструментов оптимизации</h1>
            {type === 'paid' ? (
                <a
                    style={{
                        display: 'flex',
                        justifyContent: 'center',
                        marginBottom: '32px',
                        color: 'green',
                        fontSize: '24px',
                    }}
                    href={'https://onlinetestpad.com/ru/test/985652-test-na-risk-profil-investora'}
                    target={'_blank'}>
                    Пройти инвесттестирование
                </a>
            ) : (
                ''
            )}
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'space-evenly',
                    marginBottom: '64px',
                }}>
                <>
                    <MultipleSelect data={Store.getCurrencys} title="Выберите активы" setCurrentValue={setActives} />
                    <SelectOne
                        data={type === 'free' ? optimizationTypeFree : optimizationTypePaid}
                        title="Выберите вариант оптимизации"
                        setCurrentValue={setTypeOptimizationValue}
                    />
                </>
            </div>
            <div style={{ display: 'flex', justifyContent: 'center', marginBottom: '24px' }}>
                <ButtonSend title={'Рассчитать оптимизацию'} onClick={() => makeFreeOptimization(actives, typeOptimizationValue)} />
            </div>
            <div style={{ display: 'flex', justifyContent: 'center' }}>
                <ButtonBack
                    style={{ width: '100%', backgroundColor: 'green' }}
                    title={'Вернуться на главную'}
                    onClick={() => {
                        Store.currentPage = 'greeting';
                        Store.setResultWeight = [];
                        Store.setCurrentOptimizationType = 'free';
                        Store.setCurrencys = [];
                        Store.setCurrency = '';
                    }}
                />
            </div>
        </div>
    );
});

export default Optimization;
